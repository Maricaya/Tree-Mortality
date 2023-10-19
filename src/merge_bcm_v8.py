#!/usr/bin/env python
import click
from pathlib import Path
import json
import xarray as xr
from dask.diagnostics import ProgressBar


@click.command()
@click.argument('netcdffiles', nargs=-1, type=click.Path(
    path_type=Path, exists=True
))
@click.argument('zarrfile', type=click.Path(
    path_type=Path, exists=False
))
@click.option('-c', '--configfile', default=None,
    type=click.Path(path_type=Path, exists=True)
)
def main(netcdffiles, zarrfile, configfile):

    if configfile is not None:
        with open(configfile, 'r') as f:
            config = json.load(f)
    else:
        config = {}

    chunks = config.get('chunks', None)

    ds = xr.open_mfdataset(
        netcdffiles, engine='h5netcdf', parallel=True, join='override'
    )

    if chunks is not None:
        ds = ds.chunk(chunks)

    write_job = ds.to_zarr(
        zarrfile, mode='w', compute=False, consolidated=True
    )

    with ProgressBar():
        write_job.persist()


if __name__ == '__main__':
    main()
