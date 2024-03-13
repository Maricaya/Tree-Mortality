#!/usr/bin/env python
import os
import click
import numpy as np
import xarray as xr
import rioxarray
from pathlib import Path
from tqdm import tqdm
from dask.diagnostics import ProgressBar


@click.command()
@click.argument('resultfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('mortalityfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(resultfile, mortalityfile, outputfile):

    results = np.load(resultfile)
    r_years = results['years'].tolist()
    ids = results['ids']
    preds = results['predictions']
    ds = xr.open_zarr(mortalityfile)

    I = np.array(ds['id'].values)
    print(I.shape)
    ii = I.ravel(order='F')
    print(ii)
    print(ii.reshape(I.shape, order='F'))

    new_tpa = []
    for y in tqdm(ds.year.values, 'Reshaping'):
        y_idx = r_years.index(y)
        p_year = preds[y_idx, y_idx]
        tpa_y_arr = np.array(ds['tpa'].sel(year=y).values)
        print(tpa_y_arr.shape)
        tpa_y = tpa_y_arr.ravel(order='C')
        new_values = np.nan * np.zeros((tpa_y_arr.size,))
        new_values[ids] = p_year
        new_values[np.isnan(tpa_y)] = np.nan
        N = new_values.reshape(tpa_y_arr.shape, order='C')
        new_tpa.append(N)

    ds = ds.assign(tpa=(('northing', 'easting', 'year'), np.dstack(new_tpa)))

    write_job = ds.to_zarr(
        outputfile, mode='w', compute=False, consolidated=True
    )

    with ProgressBar():
        write_job.persist()


if __name__ == '__main__':
    main()
