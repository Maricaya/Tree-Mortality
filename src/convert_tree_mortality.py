#!/usr/bin/env python
import os
import json
import click
import numpy as np
import xarray as xr
import rasterio as rio
import rioxarray as rxr
from pathlib import Path
from tqdm import tqdm
from dask.diagnostics import ProgressBar


def load_config(configfile):
    with open(configfile, 'r') as f:
        return json.load(f)


def load_variable(fname, vinfo, start_year):
    invar = vinfo.pop('input')
    name = vinfo.pop('name')

    ds = xr.open_dataset(fname, engine='netcdf4')

    # Select/rename variable and add attributes
    var = ds[invar].transpose('easting', 'northing', 'time')
    var = var.rename(name).assign_attrs(**vinfo)
    del var.attrs['grid_mapping']

    # Insert year values in coordinate axis
    years = np.arange(start_year, start_year + len(var.time.values))
    var = var.assign_coords(time=years).rename({ 'time': 'year' })

    return var


@click.command()
@click.argument('datadir', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('configfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(datadir, configfile, outputfile):

    config = load_config(configfile)
    start_year = config['start_year']
    vinfo = config['variables']
    chunks = config['chunks']
    pstr = config['projection']

    variables = []
    for fname, vi in tqdm(list(vinfo.items()), 'Loading Variables'):
        path = os.path.join(datadir, fname)
        variables.append(load_variable(path, vi, start_year))

    dataset = xr.merge(variables, join='exact', combine_attrs='drop')

    dataset.rio.write_crs(pstr, inplace=True)

    dataset = dataset.chunk(chunks)

    write_job = dataset.to_zarr(
        outputfile, mode='w', compute=False, consolidated=True
    )

    with ProgressBar():
        write_job.persist()


if __name__ == '__main__':
    main()
