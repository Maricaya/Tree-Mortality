#!/usr/bin/env python
import click
import numpy as np
import xarray as xr
from pickle import dump
from pathlib import Path
from sklearn.ensemble import RandomForestRegressor


from train_rf_model_ray import filter_inf


@click.command()
@click.argument('trainingfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('modelfile', type=click.Path(
    path_type=Path, exists=False
))
@click.option('-y', '--year', default=2012, type=int)
def main(trainingfile, modelfile, year):

    ds = xr.open_zarr(trainingfile)

    idx = (ds['year'] == year).compute()

    ds_year = ds.where(idx, drop=True).compute()

    ytrn = ds_year['tpa'].as_numpy()
    features = ds_year.drop_vars(
        ('id', 'fold', 'easting', 'northing', 'year', 'tpa')
    )
    feature_names = list(features.keys())
    Xtrn = features.to_array().T
    Xtrn, ytrn = filter_inf(Xtrn, ytrn)

    rf = RandomForestRegressor(max_depth=5)
    rf.fit(Xtrn, ytrn)

    output = {
        'model': rf,
        'features': feature_names,
    }

    with open(modelfile, 'wb') as f:
        dump(output, f)


if __name__ == '__main__':
    main()
