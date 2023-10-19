#!/usr/bin/env python
import click
import json
from pathlib import Path
import xarray as xr
from dask.distributed import Client


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

    client = Client()

    ds = xr.open_zarr(zarrfile)

    if chunk_config is not None:
        ds = ds.chunk(chunk_config)

    write_job = ds.to_netcdf(
        nc4file, engine='h5netcdf',
        encoding={
            v: {"compression": "gzip", "compression_opts": 9}
            for v in ds.data_vars
        },
        compute=False
    )

    print(f'Writing data, view progress: {client.dashboard_link}')
    write_job.compute()
    print('Done')


if __name__ == '__main__':
    main()
