#!/usr/bin/env python
import os
import json
import click
import numpy as np
import xarray as xr
import rasterio as rio
import geopandas as gpd
import rioxarray as rxr
from pathlib import Path
from tqdm import tqdm
from dask.diagnostics import ProgressBar


def load_config(configfile):
    with open(configfile, 'r') as f:
        return json.load(f)


def load_variable(fname, vinfo, years):
    invar = vinfo.pop('input')
    name = vinfo.pop('name')

    ds = xr.open_dataset(fname, engine='netcdf4')

    # Select/rename variable and add attributes
    var = ds[invar].transpose('northing', 'easting', 'time')
    var = var.rename(name).assign_attrs(**vinfo)
    del var.attrs['grid_mapping']

    # Insert year values in coordinate axis
    var = var.assign_coords(time=years).rename({ 'time': 'year' })

    return var


def load_annual_shapefiles(datadir, shapefile_fmt, default_shapefile, years):
    if default_shapefile is None:
        default_region_df = None
    else:
        default_region_df = gpd.read_file(os.path.join(datadir, default_shapefile))

    annual_shapefiles = [
        os.path.join(datadir, shapefile_fmt.format(year=y))
        for y in years
    ]

    region_df_dict = {
        year: gpd.read_file(sf)
        for year, sf in tqdm(list(zip(years, annual_shapefiles)), 'Loading Shapefiles')
        if os.path.exists(sf)
    } if shapefile_fmt is not None else {}

    return region_df_dict, default_region_df


def apply_masks(dataset, region_df_dict, default_region_df, years):

    # If no shapefiles are provided, do not modify dataset
    if len(region_df_dict) == 0 and default_region_df is None:
        return dataset

    print('Fill NaN...')
    dataset = dataset.fillna(0.0)
    print('...done.')
    dataset = dataset.rename({ 'easting': 'x', 'northing': 'y' })

    if len(region_df_dict) == 0:
        dataset = dataset.rio.clip(
            default_region_df.geometry.values, default_region_df.crs
        )

    else:
        dataset = xr.concat([
            dataset.sel(year=y).rio.clip(
                region_df_dict.get(y, default_region_df).geometry.values,
                region_df_dict.get(y, default_region_df).crs
            )
            for y in tqdm(years, 'Applying Masks')
            ],
            dim='year',
            data_vars='all',
            coords='different',
        )

    dataset = dataset.rename({ 'x': 'easting', 'y': 'northing' })
    return dataset


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
    years = config['years']
    vinfo = config['variables']
    chunks = config['chunks']
    pstr = config['projection']
    region_shapefile = config.get('region_shapefile', None)
    annual_region_shapefile_fmt = config.get('region_annual_shapefile', None)

    variables = []
    for fname, vi in tqdm(list(vinfo.items()), 'Loading Variables'):
        path = os.path.join(datadir, fname)
        variables.append(load_variable(path, vi, years))

    dataset = xr.merge(variables, join='exact', combine_attrs='drop')
    years = dataset.year.values

    region_df_dict, default_region_df = load_annual_shapefiles(
        datadir, annual_region_shapefile_fmt, region_shapefile, years
    )

    dataset.rio.write_crs(pstr, inplace=True)

    dataset = apply_masks(dataset, region_df_dict, default_region_df, years)

    dataset = dataset.chunk(chunks)

    write_job = dataset.to_zarr(
        outputfile, mode='w', compute=False, consolidated=True
    )

    with ProgressBar():
        write_job.persist()


if __name__ == '__main__':
    main()
