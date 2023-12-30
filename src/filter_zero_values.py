#!/usr/bin/env python
import click
import xarray as xr
from pathlib import Path
from dask.diagnostics import ProgressBar


@click.command()
@click.argument('trainingfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(trainingfile, outputfile):

    ds = xr.open_zarr(trainingfile)
    chunks = {
        k: v[0] for k, v in ds.chunksizes.items()
    }

    print(f'Selecting data...')
    new = ds.where((ds.tpa > 0).compute(), drop=True)
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
