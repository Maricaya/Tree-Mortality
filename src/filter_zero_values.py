#!/usr/bin/env python
import click
import json
import xarray as xr
from pathlib import Path
from dask.diagnostics import ProgressBar


@click.command()
@click.argument('trainingfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('configfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(trainingfile, configfile, outputfile):

    # Load config
    with open(configfile, 'r') as f:
        config = json.load(f)

    target = config['target']

    ds = xr.open_zarr(trainingfile)
    chunks = {
        k: v[0] for k, v in ds.chunksizes.items()
    }

    print(f'Selecting data...')
    new = ds.where((ds[target] > 0).compute(), drop=True)
    print('...done.')

    write_job = new.chunk(chunks).to_zarr(
        outputfile, mode='w', consolidated=True,
        compute=False
    )

    print(f'Writing data...')
    with ProgressBar():
        write_job.persist()
    print('...done.')


if __name__ == '__main__':
    main()
