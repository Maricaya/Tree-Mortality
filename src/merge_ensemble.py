#!/usr/bin/env python
import os
import json
import click
import warnings
import xarray as xr
from pathlib import Path
from dask.diagnostics import ProgressBar


@click.command()
@click.argument('projectionfiles', nargs=-1, type=click.Path(
    path_type=Path, exists=True
))
@click.argument('configfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(projectionfiles, configfile, outputfile):

    warnings.filterwarnings(
        'ignore', r'All-NaN (slice|axis) encountered'
    )

    # Load config
    with open(configfile, 'r') as f:
        config = json.load(f)

    statistics = config['statistics']
    chunks = config['chunks']

    datasets = []
    for pf in sorted(projectionfiles):
        pname = os.path.splitext(os.path.basename(pf))[0]
        ds = xr.open_zarr(pf)
        ds = ds.assign_coords(coords={ 'projection': pname })
        ds = ds.expand_dims(dim={ 'projection': 1 }, axis=0)
        datasets.append(ds)

    ds = xr.concat(
        datasets, dim='projection', coords='different', data_vars='all'
    )

    stats = [
        getattr(
            ds[s['variable']], s['statistic']
        )(
            dim='projection', **s.get('kwargs', {})
        ).rename(
            s['name']
        )
        for s in statistics
    ]
    for s, sinfo in zip(stats, statistics):
        s.attrs.update(**sinfo.get('attrs', {}))

    dataset = xr.merge(stats, join='exact', combine_attrs='drop')

    dataset.rio.write_crs(ds.rio.crs, inplace=True)

    write_job = dataset.chunk(chunks).to_zarr(
        outputfile, mode='w', compute=False, consolidated=True
    )

    with ProgressBar():
        write_job.persist()


if __name__ == '__main__':
    main()
