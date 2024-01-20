#!/usr/bin/env python
import os
import re
import json
import click
import pyproj
import numpy as np
import xarray as xr
import rasterio as rio
import rioxarray as rxr
from glob import glob
from tqdm import tqdm
from pathlib import Path
from zipfile import ZipFile
from datetime import datetime
from collections import defaultdict
from dask.diagnostics import ProgressBar


from convert_bcm_v8 import load_config, read_archive


@click.command()
@click.argument('datadir', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('bcmconfigfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(datadir, bcmconfigfile, outputfile):

    bcmconfig = load_config(bcmconfigfile)
    vinfo = bcmconfig['variables']
    pstr = bcmconfig['projection']
    chunks = bcmconfig['chunks']

    zip_files = sorted(glob(os.path.join(datadir, '*.zip')))

    datasets = []

    for zf in zip_files:
        vname, start, dataset = read_archive(vinfo, zf, chunks)
        datasets.append(dataset)

    dataset = xr.merge(datasets, join='override')
    dataset.rio.write_crs(pstr, inplace=True)

    write_job = dataset.to_zarr(
        outputfile, mode='w', compute=False, consolidated=True
    )

    with ProgressBar():
        write_job.persist()


if __name__ == '__main__':
    main()
