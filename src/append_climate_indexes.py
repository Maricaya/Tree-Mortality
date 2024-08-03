#!/usr/bin/env python
import json
import click
from pathlib import Path
import numpy as np
import xarray as xr
from tqdm import tqdm
from scipy.stats import norm, gamma
from concurrent.futures import ProcessPoolExecutor


def _compute_si(focus, ref, dist=gamma, prob_zero=False, fit_kwargs=None):
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

def compute_si_ppf(focal_period, reference_period=None, reference_dist=gamma, prob_zero: bool = False, fit_kwargs: dict = None, time_dim: str = 'year'):
    if reference_period is None:
        reference_period = focal_period

    new_time_dim = f'_new_{time_dim}'

    return xr.apply_ufunc(
        _compute_si,
        focal_period.rename({time_dim: new_time_dim}).load(),
        reference_period.load(),
        input_core_dims=[[new_time_dim], [time_dim]],
        exclude_dims=set([time_dim]),
        output_core_dims=[[new_time_dim]],
        output_dtypes=[np.float64],
        vectorize=True,
        kwargs={
            'dist': reference_dist,
            'prob_zero': prob_zero,
            'fit_kwargs': fit_kwargs,
        },
    ).rename({new_time_dim: time_dim}).assign_attrs({'units': 'standard deviations'})

def spi(focal_period, reference_period=None, time_dim='year'):
    return compute_si_ppf(
        focal_period, reference_period,
        reference_dist=gamma,
        prob_zero=True,
        fit_kwargs={'floc': 0},
        time_dim=time_dim
    ).transpose(*focal_period.dims)

def spei(focal_period, reference_period=None, time_dim='year'):
    return compute_si_ppf(
        focal_period, reference_period,
        reference_dist=gamma,
        prob_zero=False,
        time_dim=time_dim
    ).transpose(*focal_period.dims)

def pr(ds, window, precip='ppt'):
    print(f"Dataset dimensions: {ds.dims}")
    print(f"Dataset variables: {list(ds.data_vars)}")
    print(f"Selected variable shape: {ds[precip].shape}")
    print(f"Rolling window size: {window}")

    time_dim, window_size = next(iter(window.items()))
    time_dim_size = ds[precip].sizes[time_dim]

    if time_dim_size < window_size:
        raise ValueError(f"Window size {window_size} is too large for the data size {time_dim_size}")

    return ds[precip].rolling({time_dim: window_size}).sum().assign_attrs({'units': ds[precip].units})

def pret(ds, window, precip='ppt', et='pet'):
    wb = ds[precip] - ds[et]

    print(f"Dataset dimensions: {ds.dims}")
    print(f"Dataset variables: {list(ds.data_vars)}")
    print(f"Selected variable shape (water balance): {wb.shape}")
    print(f"Rolling window size: {window}")

    time_dim, window_size = next(iter(window.items()))
    time_dim_size = wb.sizes[time_dim]

    if time_dim_size < window_size:
        raise ValueError(f"Window size {window_size} is too large for the data size {time_dim_size}")

    return wb.rolling({time_dim: window_size}).sum().assign_attrs({'units': ds[precip].units})

def process_index(idx, ds, span, focal_period, reference_period, time_dim, computed_indices):
    name = idx['name']
    params = idx.get('params', {}).copy()
    params.pop('chunk', None)
    window = {time_dim: span}

    print(f"Processing index: {name}")
    try:
        if name == "PR":
            da = pr(ds, window, **params)
        elif name == "PRET":
            da = pret(ds, window, **params)
        elif name == "SPI":
            pr_in = computed_indices.get('PR')
            if pr_in is None:
                raise ValueError('PR index not computed for the current span.')
            foc = pr_in.sel({time_dim: slice(*focal_period)})
            ref = pr_in.sel({time_dim: slice(*reference_period)})
            da = spi(foc, ref, time_dim=time_dim)
        elif name == "SPEI":
            wb_in = computed_indices.get('PRET')
            if wb_in is None:
                raise ValueError('PRET index not computed for the current span.')
            foc = wb_in.sel({time_dim: slice(*focal_period)})
            ref = wb_in.sel({time_dim: slice(*reference_period)})
            da = spei(foc, ref, time_dim=time_dim)
        else:
            raise ValueError(f'Unknown index "{name}"')
    except ValueError as e:
        print(f"Skipping index {name} due to error: {e}")
        return None, None

    sub = da.sel({time_dim: slice(*focal_period)})
    sub = sub.rename(idx['name_format'].format(span=span))
    sub = sub.assign_attrs({
        'long_name': idx['long_name_format'].format(span=span)
    })
    return name, sub

def make_indices(ds, span, focal_period, reference_period, indices, time_dim='year'):
    computed_indices = {}
    ret_indices = []

    for idx in indices:
        name = idx['name']
        if name in ["PR", "PRET"]:
            name, result = process_index(idx, ds, span, focal_period, reference_period, time_dim, computed_indices)
            if result is not None:
                computed_indices[name] = result
                ret_indices.append(result)

    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_index, idx, ds, span, focal_period, reference_period, time_dim, computed_indices)
            for idx in indices if idx['name'] in ["SPI", "SPEI"]
        ]
        for future in tqdm(futures, desc=f'Processing Span {span} Indices'):
            name, result = future.result()
            if result is not None:
                ret_indices.append(result)
    return ret_indices

@click.command()
@click.argument('inputfile', type=click.Path(path_type=Path, exists=True))
@click.argument('configfile', type=click.Path(path_type=Path, exists=True))
@click.argument('outputfile', type=click.Path(path_type=Path, exists=False))
@click.option('-r', '--reference', type=click.Path(path_type=Path, exists=False), default=None)
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

    ds = xr.open_zarr(inputfile)
    ds = ds.chunk(chunks)

    for dim, size in ds.sizes.items():
        if size == 0:
            raise ValueError(f"The dimension {dim} in the input dataset has size 0. Cannot proceed with empty dimensions.")
    print("Dataset loaded successfully. Dimensions and sizes:")
    print(ds.sizes)
    print(ds)

    if reference is not None:
        dsref = xr.open_zarr(reference)
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
                dsref.sel({time_dim: slice(*reference_period)}),
                ds.sel({time_dim: slice(*focal_period)}),
            ],
            dim=time_dim,
            data_vars='all',
            coords='all',
            join='inner'
        )
        ds.rio.write_crs(dsref.rio.crs, inplace=True)

    orig = ds.sel({time_dim: slice(*focal_period)})

    all_indices = [orig]
    for span in spans:
        all_indices += make_indices(
            ds, span, focal_period, reference_period, indices,
            time_dim=time_dim
        )

    indices_ds = xr.merge(
        all_indices, combine_attrs='drop_conflicts'
    )
    indices_ds = indices_ds.chunk(out_chunks)
    print(indices_ds)

    indices_ds.to_zarr(
        outputfile, mode='w', consolidated=True
    )
    print('Done')

if __name__ == '__main__':
    main()