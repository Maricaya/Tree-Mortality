#!/usr/bin/env python
import click
import json
from pathlib import Path
import xarray as xr
from dask.diagnostics import ProgressBar


@click.command()
@click.argument('zarrfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('nc4file', type=click.Path(
    path_type=Path, exists=False
))
@click.option('-c', '--configfile', default=None,
    type=click.Path(path_type=Path, exists=True)
)
def main(zarrfile, nc4file, configfile):

    if configfile is not None:
        with open(configfile, 'r') as f:
            config = json.load(f)
    else:
        config = {}

    chunk_config = config.get('chunks', None)

    ds = xr.open_zarr(zarrfile)

    if chunk_config is not None:
        ds = ds.chunk(chunk_config)

    # Add compression to encoding
    comp = dict(zlib=True, complevel=9)
    for v in ds.data_vars:
        ds[v].encoding.update(comp)

    write_job = ds.to_netcdf(
        nc4file, engine='h5netcdf',
        compute=False
    )

    with ProgressBar():
        write_job.persist()


if __name__ == '__main__':
    main()
