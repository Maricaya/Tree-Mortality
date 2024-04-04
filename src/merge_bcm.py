#!/usr/bin/env python
import os
import click
import xarray as xr
from pathlib import Path
from dask.diagnostics import ProgressBar

from convert_bcm_v8 import load_config


@click.command()
@click.argument('variablefiles', nargs=-1, type=click.Path(
    path_type=Path, exists=True
))
@click.argument('configfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(variablefiles, configfile, outputfile):

    config = load_config(configfile)
    chunks = config['chunks']

    dataset = xr.open_mfdataset(
        variablefiles, join='override', parallel=True, engine='h5netcdf'
    )

    dataset = dataset.chunk(chunks)

    write_job = dataset.to_zarr(
        outputfile, mode='w', compute=False, consolidated=True
    )

    with ProgressBar():
        write_job.persist()


if __name__ == '__main__':
    main()
