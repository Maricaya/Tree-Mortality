#!/usr/bin/env python
import os
import click
import xarray as xr
import rioxarray as rxr
from pathlib import Path
from dask.diagnostics import ProgressBar

from convert_bcm_v8 import load_config


def get_variable(ds, vinfo):
    var = ds[vinfo['name']]
    if 'new_name' in vinfo:
        var = var.rename(vinfo['new_name'])
    del var.attrs['grid_mapping']
    var.attrs.update(**vinfo.get('attrs', {}))
    return var


@click.command()
@click.argument('variablefiles', nargs=-1, type=click.Path(
    path_type=Path, exists=True
))
@click.argument('bcmfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('configfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(variablefiles, bcmfile, configfile, outputfile):

    config = load_config(configfile)
    chunks = config['chunks']
    variables = config['variables']

    dataset = xr.open_mfdataset(
        variablefiles, join='override', parallel=True, engine='h5netcdf'
    )

    bcm_ds = xr.open_zarr(bcmfile)

    converted = xr.merge([
        get_variable(dataset, vinfo)
        for vinfo in variables
    ], join='exact', combine_attrs='drop_conflicts')

    converted = converted.rename({ 'easting': 'x', 'northing': 'y' })
    bcm_ds = bcm_ds.rename({ 'easting': 'x', 'northing': 'y' })

    converted.rio.write_crs(dataset.crs.proj4, inplace=True)
    reproj = converted.rio.reproject_match(bcm_ds)

    reproj = reproj.rename({ 'x': 'easting', 'y': 'northing' })
    reproj = reproj.chunk(chunks)

    write_job = reproj.to_zarr(
        outputfile, mode='w', compute=False, consolidated=True
    )

    with ProgressBar():
        write_job.persist()


if __name__ == '__main__':
    main()
