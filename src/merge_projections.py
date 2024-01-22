#!/usr/bin/env python
import os
import click
import xarray as xr
from pathlib import Path
from dask.diagnostics import ProgressBar


@click.command()
@click.argument('variablefiles', nargs=-1, type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(variablefiles, outputfile):

    dataset = xr.open_mfdataset(
        variablefiles, join='override', parallel=True, engine='h5netcdf'
    )

    write_job = dataset.to_zarr(
        outputfile, mode='w', compute=False, consolidated=True
    )

    with ProgressBar():
        write_job.persist()


if __name__ == '__main__':
    main()
