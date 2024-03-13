#!/usr/bin/env python
import os
import PIL
import json
import click
import numpy as np
import xarray as xr
import rioxarray
import geopandas as gpd
from pathlib import Path
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from convert_tree_mortality import apply_masks


@click.command()
@click.argument('climatefile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('histfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('projfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('shapefile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(climatefile, histfile, projfile, shapefile, outputfile):

    shape_df = gpd.read_file(shapefile)

    var = 'PR1'
    var = 'SPI1'
    clim = xr.open_zarr(climatefile)[var]
    hist = xr.open_zarr(histfile)
    ds = xr.open_zarr(projfile)

    clim = apply_masks(clim, {}, shape_df, [])
    #hist = apply_masks(hist, {}, shape_df, [])
    ds = apply_masks(ds, {}, shape_df, [])

    med = ds.quantile(0.5, dim=('easting', 'northing'), skipna=True)
    med_hist = hist.chunk(
        {
            'northing': -1,
            'easting': -1,
            'year': 1,
        }
    ).quantile(0.5, dim=('easting', 'northing'), skipna=True)

    med_clim = clim.chunk(
        {
            'northing': -1,
            'easting': -1,
            'year': 1,
        }
    ).mean(dim=('easting', 'northing'), skipna=True)
    print(med_clim.year.values)

    fig = plt.figure(figsize=(16, 6))
    ax = fig.add_subplot(111)

    ax.plot(med_hist.year.values, med_hist['tpa'].values, 'g-', lw=3, label='Tree Mortality (historical)')
    ax.plot(med.year.values, med['tpa_projection_median'].values, 'g--', lw=3, label='Tree Mortality (projected)')
    ax.patch.set_facecolor('none')
    ax.set_ylim(0, 10)

    ax2 = ax.twinx()
    ax2.bar(med_clim.year.values, med_clim.values, facecolor='gray', edgecolor='none', alpha=0.7)

    #ax2.plot(med_clim.year.values, med_clim['SPEI2'].values, 'r-', label='SPEI')
    ax.legend(loc='upper right', fontsize=14)

    ax.set_xlim(2010, 2100)
    ax2.set_xlim(2010, 2100)
    ax.set_xlabel('Year', fontsize=16)
    ax.set_ylabel('Annual Tree Mortality (trees/acre)', fontsize=16, color='g')
    if var == 'PR1':
        ax2.set_ylabel('Annual Precipitation (mm)', fontsize=16)
    else:
        ax2.set_ylabel('Standardized Precipitation Index (z-score)', fontsize=16)
        ax2.set_ylim(-3, 3)
    ax.tick_params(axis='y', labelcolor='g')
    ax2.set_zorder(-10)

    fig.savefig(outputfile)



if __name__ == '__main__':
    main()
