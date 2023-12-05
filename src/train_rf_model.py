#!/usr/bin/env python
import json
import click
import numpy as np
import xarray as xr
from tqdm import tqdm
from pathlib import Path
from sklearn.model_selection import GroupShuffleSplit
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error as mse, r2_score


def filter_inf(X, y, fill=1e2):
    assert not np.any(np.isnan(X))
    Xnew = np.nan_to_num(X, neginf=-fill, posinf=fill)
    return Xnew, y


@click.command()
@click.argument('trainingfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('resultfile', type=click.Path(
    path_type=Path, exists=False
))
@click.option('-y', '--year', default=2012, type=int)
@click.option('-s', '--splits', default=5, type=int)
def main(trainingfile, resultfile, year, splits):

    ds = xr.open_zarr(trainingfile)

    idx = (ds['year'] == year).compute()

    ds_year = ds.where(idx, drop=True).compute()

    groups = ds_year['fold'].to_numpy().astype(int)
    y = ds_year['tpa'].as_numpy()
    X = ds_year.drop_vars(
        ('id', 'fold', 'easting', 'northing', 'year', 'tpa')
    ).to_array().T

    gss = GroupShuffleSplit(n_splits=splits, test_size=0.1, random_state=0)

    rmses = []
    r2s = []

    iters = enumerate(gss.split(X, y, groups))
    for i, (trn_idx, tst_idx) in tqdm(list(iters), 'Bootstrapping'):
        Xtrn, ytrn = filter_inf(X[trn_idx], y[trn_idx])
        Xtst, ytst = filter_inf(X[tst_idx], y[tst_idx])

        rf = RandomForestRegressor(max_depth=5)
        rf.fit(Xtrn, ytrn)

        ypred = rf.predict(Xtst)

        rmses.append(mse(ytst, ypred, squared=False))
        r2s.append(r2_score(ytst, ypred))
        #print(f'Fold {i} of {splits}:')
        #print(f'RMSE: {rmse}')
        #print(f'R^2: {r2}')

    result = {
        'year': year,
        'splits': splits,
        'rmse': rmses,
        'r2': r2s,
    }

    with open(resultfile, 'w') as f:
        json.dump(result, f, indent=2)


if __name__ == '__main__':
    main()
