#!/usr/bin/env python
import click
from pathlib import Path
import xarray as xr
from dask.diagnostics import ProgressBar


@click.command()
@click.argument('netcdffiles', nargs=-1, type=click.Path(
    path_type=Path, exists=True
))
@click.argument('zarrfile', type=click.Path(
    path_type=Path, exists=False
))
def main(netcdffiles, zarrfile):

    chunks = { 'time': 5 }

    ds = xr.open_mfdataset(netcdffiles, parallel=True, join='override').chunk(chunks)
    write_job = ds.to_zarr(zarrfile, mode='w', compute=False, consolidated=True)

    with ProgressBar():
        write_job.persist()


if __name__ == '__main__':
    main()
