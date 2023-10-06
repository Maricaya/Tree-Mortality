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
        ds = ds.reindex(time=to_water_year(ds.time.values))

        aggregated = xr.merge([
            getattr(ds[var].groupby('time.year'), afunc)(dim='time', skipna=False)
            for var, afunc in tqdm(sorted(aggregation_funcs.items()), 'Grouping')
        ], combine_attrs='drop_conflicts')

        out_chunks['year'] = len(aggregated.year)
        aggregated = aggregated.chunk(out_chunks)

        write_job = aggregated.to_zarr(outputfile, mode='w', compute=False, consolidated=True)
        with ProgressBar():
            write_job.persist()


if __name__ == '__main__':
    main()
