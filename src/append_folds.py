#!/usr/bin/env python
import os
import json
import click
import numpy as np
import xarray as xr
import rioxarray
from pathlib import Path
from tqdm import tqdm
from dask.diagnostics import ProgressBar

import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature


def load_config(configfile):
    with open(configfile, 'r') as f:
        return json.load(f)


@click.command()
@click.argument('inputfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('configfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(inputfile, configfile, outputfile):

    with open(configfile, 'r') as f:
        config = json.load(f)

    grid_size = config['grid_size']

    ds = xr.open_zarr(inputfile).sel(year=2019)
    tpa = ds['tpa']

    fig, ax = plt.subplots(1, 1, subplot_kw=dict(projection=ccrs.Mercator()))

    ds_crs = ccrs.Projection(ds.rio.crs)
    tpa.plot.pcolormesh(x='easting', y='northing', transform=ds_crs, ax=ax)
    ax.add_feature(cfeature.STATES.with_scale('50m'))

    xs = np.array(tpa.easting.values)
    ys = np.array(tpa.northing.values)

    for x in xs[::grid_size]:
        ax.plot([x, x], [ys.min(), ys.max()], 'k-', transform=ds_crs, alpha=0.2)

    for y in ys[::grid_size]:
        ax.plot([xs.min(), xs.max()], [y, y], 'k-', transform=ds_crs, alpha=0.2)

    plt.show()


if __name__ == '__main__':
    main()
