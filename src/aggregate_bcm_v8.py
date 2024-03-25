#!/usr/bin/env python
import json
import click
from pathlib import Path
import numpy as np
import xarray as xr
from tqdm import tqdm
from dask.diagnostics import ProgressBar


WY_DETLA = np.timedelta64(3, 'M').astype('timedelta64[M]')


def to_water_year(dt):
    return (dt.astype('datetime64[M]') + WY_DETLA).astype('datetime64[ns]')


def aggregate(ds, **kwargs):
    return xr.merge([
        getattr(ds[var], afunc)(dim='time', skipna=False, keep_attrs=True)
        for var, afunc in kwargs.items()
    ], combine_attrs='drop_conflicts')


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
def main(inputfile, configfile, outputfile):

    with open(configfile, 'r') as f:
        config = json.load(f)

    chunks = config['chunks']
    out_chunks = config['output_chunks']
    aggregation_funcs = config['aggregation']

    with xr.open_zarr(inputfile) as ds:
        ds = ds.chunk(chunks)
        print('Convert years to water years...')
        ds = ds.assign_coords(time=to_water_year(ds.time.values))

        print('Apply grouping...')
        grouped = ds.groupby('time.year')

        print('Apply aggregation functions...')
        aggregated = grouped.map(aggregate, **aggregation_funcs)

        print('Chunking data...')
        out_chunks['year'] = len(aggregated.year)
        aggregated = aggregated.chunk(out_chunks)

        print('Creating write job...')
        write_job = aggregated.to_zarr(
            outputfile, mode='w', compute=False, consolidated=True
        )

        print('Writing output...')
        with ProgressBar():
            write_job.compute()
        print('Done')


if __name__ == '__main__':
    main()
