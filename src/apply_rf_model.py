#!/usr/bin/env python
import re
import ray
import json
import click
import numpy as np
import xarray as xr
import rioxarray
from pathlib import Path
from skops.io import load
from dask.diagnostics import ProgressBar

from train_rf_model_ray import filter_inf


def features_to_info(feature_names):
    feature_info = {}
    for f in feature_names:
        match = re.match(f, r'[A-Z]+(\d+)-(\d+)')
        if match is not None:
            feature_info.append(
            
            )
        else:
            raise ValueError(f'Unhandled feature "{f}"')


def get_feature(clim, year, feature_name):
    match = re.match(r'([A-Z]+)(\d+)-(\d+)', feature_name)
    if match is not None:
        fbase = match.group(1)
        cumulative = int(match.group(2))
        back = int(match.group(3))
        fname = f'{fbase}{cumulative}'
        cyear = clim.sel(year=(year - back)).assign_coords(year=[year])
        return cyear[fname].to_numpy()
    else:
        raise ValueError(f'Unhandled feature "{feature_name}"')


@ray.remote
def apply_model(climatefile, modelfile, year):

    with open(modelfile, 'rb') as f:
        model_info = load(f)

    features = model_info['features']
    model = model_info['model']

    with xr.open_zarr(climatefile) as clim:

        cube = np.dstack([
            get_feature(clim, year, f)
            for f in features
        ])
        X = cube.reshape((-1, len(features)))
        y = np.full((X.shape[0],), np.nan)
        good = np.logical_not(np.any(np.isnan(X), axis=1))
        X_good = filter_inf(X[good])

        y_good = model.predict(X_good)

        y[good] = y_good
        Y = y.reshape(cube.shape[:2])

        return Y


@click.command()
@click.argument('climatefile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('modelfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('configfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(climatefile, modelfile, configfile, outputfile):

    # Load config
    with open(configfile, 'r') as f:
        config = json.load(f)

    chunks = config['chunks']
    vname = config['prediction_variable']
    vinfo = config['prediction_variable_info']
    year_range = config['year_range']
    years = list(range(year_range['start'], year_range['end']))

    clim = xr.open_zarr(climatefile)

    ray.init(num_cpus=5)

    predictions = np.dstack(ray.get(
        [
            apply_model.remote(climatefile, modelfile, year)
            for year in years
        ]
    ))

    dataset = xr.Dataset(
        data_vars={
            vname: (
                ['northing', 'easting', 'year'],
                predictions,
                vinfo
            )
        },
        coords={
            'northing': clim.northing,
            'easting': clim.easting,
            'year': xr.Variable('year', years),
        }
    )

    dataset.rio.write_crs(clim.rio.crs, inplace=True)

    write_job = dataset.chunk(chunks).to_zarr(
        outputfile, mode='w', compute=False, consolidated=True
    )

    print(f'Writing data...')
    with ProgressBar():
        write_job.persist()
    print('Done')


if __name__ == '__main__':
    main()
