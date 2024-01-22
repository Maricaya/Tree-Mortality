#!/usr/bin/env python
import os
import re
import click
import rioxarray as rxr
from pathlib import Path
from dask.diagnostics import ProgressBar


from convert_bcm_v8 import load_config, read_archive


@click.command()
@click.argument('zipfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('configfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(zipfile, configfile, outputfile):

    config = load_config(configfile)
    vinfo = config['variables']
    pstr = config['projection']
    chunks = config['chunks']

    _, _, dataset = read_archive(vinfo, zipfile, chunks)

    dataset.rio.write_crs(pstr, inplace=True)

    # Add compression to encoding
    comp = dict(zlib=True, complevel=9)
    for v in dataset.data_vars:
        dataset[v].encoding.update(comp)

    write_job = dataset.to_netcdf(
        outputfile, engine='h5netcdf', compute=False
    )

    with ProgressBar():
        write_job.persist()


if __name__ == '__main__':
    main()
