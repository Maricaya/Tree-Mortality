#!/usr/bin/env python
import os
import json
import click
import warnings
import xarray as xr
from pathlib import Path
from dask.diagnostics import ProgressBar


@click.command()
@click.argument('projectionfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('configfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(projectionfile, configfile, outputfile):

    warnings.filterwarnings(
        'ignore', r'All-NaN (slice|axis) encountered'
    )

    # Load config
    with open(configfile, 'r') as f:
        config = json.load(f)

    statistics = config['statistics']
    chunks = config['chunks']

    ds = xr.open_zarr(projectionfile)

    stats = [
        getattr(
            ds[s['variable']], s['statistic']
        )(
            dim='year', **s.get('kwargs', {})
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
