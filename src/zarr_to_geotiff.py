#!/usr/bin/env python
import json
import click
import xarray as xr
from tqdm import tqdm
from pathlib import Path
from itertools import product
from werkzeug.security import safe_join


@click.command()
@click.argument('zarrfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputdir', type=click.Path(
    path_type=Path, exists=False
))
@click.option('-c', '--configfile', default=None,
    type=click.Path(path_type=Path, exists=True)
)
def main(zarrfile, outputdir, configfile):

    if configfile is not None:
        with open(configfile, 'r') as f:
            config = json.load(f)
    else:
        config = {}

    file_fmt = config.get('filename_format', '{variable}_{year}.tif')

    ds = xr.open_zarr(zarrfile)

    tasks = []

    pairs = list(product(ds.data_vars, ds.year.values))

    for var, year in tqdm(pairs, 'Converting'):
        ofile = file_fmt.format(variable=var, year=year)
        opath = safe_join(outputdir, ofile)

        da = ds[var]
        dat = da.sel(year=year)
        dat = dat.rename(northing='y', easting='x')
        dat = dat.assign_attrs(year=year, short_name=var)
        dat.rio.to_raster(
            opath, compute=True, compress='LZMA'
        )


if __name__ == '__main__':
    main()
