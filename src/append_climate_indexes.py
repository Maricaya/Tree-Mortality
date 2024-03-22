#!/usr/bin/env python
import json
import click
from pathlib import Path
import numpy as np
import xarray as xr
from tqdm import tqdm
from scipy.stats import norm, gamma, fisk
from dask.distributed import Client
from dask_jobqueue import SLURMCluster


def _compute_si(focus, ref, dist=gamma, prob_zero=False, fit_kwargs=None):

    # Handle NaN values
    nan_values = np.isnan(ref)
    if np.all(nan_values):
        return np.full(focus.shape, np.nan)
    elif np.any(nan_values):
        ref = ref[np.logical_not(nan_values)]

    if fit_kwargs is None:
        fit_kwargs = {}

    if prob_zero:
        p0 = np.average(ref == 0)
        params = dist.fit(ref[ref != 0], **fit_kwargs)
        cdf_sub = dist.cdf(focus, *params)
        cdf = p0 + (1 - p0) * cdf_sub
        cdf[focus == 0] = p0

    else:
        params = dist.fit(ref, **fit_kwargs)
        cdf = dist.cdf(focus, *params)

    return norm.ppf(cdf)


def compute_si_ppf(focal_period, reference_period=None,
        reference_dist=gamma, prob_zero: bool = False, fit_kwargs: dict = None,
        time_dim: str = 'year', chunk: dict = None):

    if reference_period is None:
        reference_period = focal_period

    new_time_dim = f'_new_{time_dim}'

    if chunk is not None:
        focal_period = focal_period.chunk(chunk)
        reference_period = reference_period.chunk(chunk)

    return xr.apply_ufunc(
        _compute_si,
        focal_period.rename({time_dim : new_time_dim}),
        reference_period,
        input_core_dims=[[new_time_dim], [time_dim]],
        exclude_dims=set([time_dim]),
        output_core_dims=[[new_time_dim]],
        output_dtypes=[np.float64],
        vectorize=True,
        dask='parallelized',
        kwargs={
            'dist': reference_dist,
            'prob_zero': prob_zero,
            'fit_kwargs': fit_kwargs,
        },
    ).rename({new_time_dim : time_dim}).assign_attrs(
        {  'units': 'standard deviations' }
    )


def spi(focal_period, reference_period=None, time_dim='year', chunk=None):
    return compute_si_ppf(
        focal_period, reference_period,
        reference_dist=gamma,
        prob_zero=True,
        fit_kwargs={ 'floc': 0 },
        time_dim=time_dim,
        chunk=chunk
    ).transpose(*focal_period.dims)


def spei(focal_period, reference_period=None, time_dim='year', chunk=None):
    return compute_si_ppf(
        focal_period, reference_period,
        reference_dist=gamma,
        prob_zero=False,
        time_dim=time_dim,
        chunk=chunk
    ).transpose(*focal_period.dims)


def pr(ds, window, precip='ppt'):
    return ds[precip].rolling(window).sum().assign_attrs(
        {'units': ds[precip].units}
    )


def pret(ds, window, precip='ppt', et='pet'):
    wb = ds[precip] - ds[et]
    return wb.rolling(window).sum().assign_attrs(
        {'units': ds[precip].units}
    )


def make_indices(ds, span, focal_period, reference_period, indices, time_dim='year'):
    inputs = {}
    ret_indices = []

    for idx in indices:
        name = idx['name']
        params = idx.get('params', {})
        window = { time_dim : span }
        match name:
            case "PR":
                da = pr(ds, window, **params)

            case "PRET":
                da = pret(ds, window, **params)

            case "SPI":
                pr_in = inputs['PR']
                foc = pr_in.sel({ time_dim: slice(*focal_period) })
                ref = pr_in.sel({ time_dim: slice(*reference_period) })
                da = spi(foc, ref, time_dim=time_dim, **params)

            case "SPEI":
                wb_in = inputs['PRET']
                foc = wb_in.sel({ time_dim: slice(*focal_period) })
                ref = wb_in.sel({ time_dim: slice(*reference_period) })
                da = spei(foc, ref, time_dim=time_dim, **params)

            case _:
                raise ValueError(f'Unknown index "{name}"')

        inputs[name] = da
        sub = da.sel({ time_dim: slice(*focal_period) })
        sub = sub.rename(idx['name_format'].format(span=span))
        sub = sub.assign_attrs({
            'long_name': idx['long_name_format'].format(span=span)
        })
        ret_indices.append(sub)

    return ret_indices


@click.command()
@click.argument('inputfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('configfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
@click.option('-r', '--reference', type=click.Path(
    path_type=Path, exists=False
), default=None)
def main(inputfile, configfile, outputfile, reference):

    with open(configfile, 'r') as f:
        config = json.load(f)

    reference_period = config['reference_period']
    focal_period = config['focal_period']
    spans = config['spans']
    time_dim = config['time_dim']
    indices = config['indices']
    chunks = config['chunks']
    out_chunks = config['output_chunks']

    if 'dask_slurm' in config:
        cluster = SLURMCluster(**config['dask_slurm'])
        client = Client(cluster)

    else:
        dask_client = config.get('dask_client', {})
        # Start Dask client for monitoring
        client = Client(**dask_client)

    ds = xr.open_zarr(inputfile)
    # Re-chunk data
    ds = ds.chunk(chunks)

    if reference is not None:
        dsref = xr.open_zarr(reference)

        # Re-chunk reference data
        dsref = dsref.chunk(chunks)

        ds = ds.assign_coords(
            easting=np.round(ds.easting.values, 4),
            northing=np.round(ds.northing.values, 4),
        )
        dsref = dsref.assign_coords(
            easting=np.round(dsref.easting.values, 4),
            northing=np.round(dsref.northing.values, 4),
        )

        ds = xr.concat(
            [
                dsref.sel({ time_dim: slice(*reference_period) }),
                ds.sel({ time_dim: slice(*focal_period) }),
            ],
            dim=time_dim,
            data_vars='all',
            coords='all',
            join='inner'
        )
        ds.rio.write_crs(dsref.rio.crs, inplace=True)

        ds = ds.chunk(chunks)

    orig = ds.sel({ time_dim: slice(*focal_period) })

    all_indices = [orig]
    for span in tqdm(spans, 'Computing Span Indices'):
        all_indices += make_indices(
            ds, span, focal_period, reference_period, indices,
            time_dim=time_dim
        )

    indices_ds = xr.merge(
        all_indices, combine_attrs='drop_conflicts'
    )
    indices_ds = indices_ds.chunk(out_chunks)
    print(indices_ds)

    write_job = indices_ds.to_zarr(
        outputfile, mode='w', compute=False, consolidated=True
    )

    print(f'Writing data, view progress: {client.dashboard_link}')
    write_job.compute()
    print('Done')


if __name__ == '__main__':
    main()
