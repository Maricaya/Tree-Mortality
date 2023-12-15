#!/usr/bin/env python
import click
import numpy as np
import xarray as xr
from tqdm import tqdm
from pathlib import Path
from collections import defaultdict
from sklearn.model_selection import LeaveOneGroupOut
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
def main(trainingfile, resultfile, year):

    ds = xr.open_zarr(trainingfile)

    idx = (ds['year'] == year).compute()

    ds_year = ds.where(idx, drop=True).compute()

    groups = ds_year['fold'].to_numpy().astype(int)
    ids = ds_year['id'].as_numpy()
    northings = ds_year['northing'].as_numpy()
    eastings = ds_year['easting'].as_numpy()
    y = ds_year['tpa'].as_numpy()
    X = ds_year.drop_vars(
        ('id', 'fold', 'easting', 'northing', 'year', 'tpa')
    ).to_array().T

    gss = LeaveOneGroupOut()

    result = defaultdict(list)

    iters = enumerate(gss.split(X, y, groups))
    for i, (trn_idx, tst_idx) in tqdm(list(iters), 'Leave-One-Out'):
        Xtrn, ytrn = filter_inf(X[trn_idx], y[trn_idx])
        Xtst, ytst = filter_inf(X[tst_idx], y[tst_idx])
        result['id'].append(ids[tst_idx])
        result['northing'].append(northings[tst_idx])
        result['easting'].append(eastings[tst_idx])
        result['true'].append(ytst)

        rf = RandomForestRegressor(max_depth=5)
        rf.fit(Xtrn, ytrn)

        ypred = rf.predict(Xtst)
        result['pred'].append(ypred)

        print(mse(ytst, ypred, squared=False))

    result_stacked = {
        k: np.hstack(v) for k, v in result.items()
    }

    np.savez_compressed(resultfile, **result_stacked)


if __name__ == '__main__':
    main()
