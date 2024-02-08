#!/usr/bin/env python
import os
import PIL
import json
import click
import numpy as np
import xarray as xr
import rioxarray
from pathlib import Path
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


@click.command()
@click.argument('climatefile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('datafile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(climatefile, datafile, outputfile):

    clim = xr.open_zarr(climatefile)
    ds = xr.open_zarr(datafile)

    med = ds.quantile(0.5, dim=('easting', 'northing'), skipna=True)
    qlo = ds.quantile(0.25, dim=('easting', 'northing'), skipna=True)
    qhi = ds.quantile(0.75, dim=('easting', 'northing'), skipna=True)

    med_clim = clim.chunk(
        {
            'northing': -1,
            'easting': -1,
            'year': 1,
        }
    ).quantile(0.5, dim=('easting', 'northing'), skipna=True)

    fig = plt.figure(figsize=(16, 6))
    ax = fig.add_subplot(111)
    ax.fill_between(
        med.year.values,
        qlo['tpa_projection_median'].values, qhi['tpa_projection_median'].values,
        facecolor='gray', edgecolor='none', alpha=0.5
    )
    ax.plot(med.year.values, med['tpa_projection_median'].values, 'k-')
    ax.set_ylim(0, 8)

    ax2 = ax.twinx()
    #ax2.plot(med_clim.year.values, med_clim['SPI2'].values, 'b-', lw=3, label='SPI')
    ax2.plot(med_clim.year.values + 3, -med_clim['SPI2'].values, 'r-', lw=3, label='SPI')
    #ax2.plot(med_clim.year.values, med_clim['SPEI2'].values, 'r-', label='SPEI')
    ax2.set_ylim(-3, 3)
    #ax2.legend(loc='lower left', ncol=2, fontsize=14)

    ax.set_xlim(2025, 2100)
    ax2.set_xlim(2025, 2100)
    ax.set_xlabel('Year', fontsize=16)
    ax.set_ylabel('Annual Tree Mortality (trees/acre)', fontsize=16)
    #ax2.set_ylabel('Standardized Precipitation Index (z-score)', fontsize=16, color='b')
    ax2.set_ylabel('Standardized Precipitation Index (z-score)', fontsize=16, color='r')
    ax2.tick_params(axis='y', labelcolor='r')

    fig.savefig(outputfile)



if __name__ == '__main__':
    main()
