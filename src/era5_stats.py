#!/usr/bin/env python
import os
import click
import warnings
import numpy as np
import xarray as xr
from tqdm import tqdm
from pathlib import Path
from werkzeug.security import safe_join
from dask.diagnostics import ProgressBar

from util import load_config

DEFAULT_SHIFT = np.timedelta64(1, 'm').astype('timedelta64[m]')


def shift_backwards(dt, shift=DEFAULT_SHIFT):
    return (dt.astype('datetime64[m]') - shift).astype('datetime64[ns]')


def adjust_leap_year(ds):
    if ds.dims['dayofyear'] > 365:
        ds = ds.drop_sel(dayofyear=60)
        ds = ds.assign_coords(dayofyear=np.arange(1, 366, dtype=int))
    return ds


def get_annual_stats(ds, year, stats):
    ds = ds.sel(time=(ds.time.dt.year == year))
    groups = ds.groupby('time.dayofyear')
    stat_vars = []
    for sd in stats:
        var = sd['variable']
        name = sd['name']
        function = sd['function']
        kwargs = sd.get('kwargs', {})
        stat_var = getattr(groups, function)(**kwargs)[var]
        stat_var.attrs.update(**sd.get('attrs', {}))
        stat_vars.append(stat_var.rename(name))
    annual_stats = xr.merge(stat_vars, combine_attrs='drop_conflicts')
    annual_stats = adjust_leap_year(annual_stats)
    return annual_stats.expand_dims(dim={'year': [year]}, axis=1)


@click.command()
@click.argument('eradir', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('configfile', type=click.Path(
    path_type=Path, exists=False
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(eradir, configfile, outputfile):

    warnings.filterwarnings(
        'ignore', r'All-NaN (slice|axis) encountered'
    )

    config = load_config(configfile)
    year_range = config['years']
    suffix = config.get('suffix', 'nc')
    stats = config['stats']
    chunks = config['chunks']

    # Need to add 2: 1 so it is inclusive of the end year, and another 1 so the
    # first hour of the following year in shifted backwards to the end year
    year_range_args = [year_range[0], year_range[1] + 2]

    erafiles = [
        safe_join(eradir, f'{year}.{suffix}')
        for year in range(*year_range_args)
    ]

    missing = [
        f for f in erafiles
        if not os.path.exists(f)
    ]
    if len(missing) > 0:
        raise ValueError(f'Missing the following files: {", ".join(missing)}')

    erads = xr.open_mfdataset(erafiles, join='override', parallel=True)

    # Shift times backwards by 1 hour so midnight belongs to previous day
    new_times = shift_backwards(erads.time.values)
    erads = erads.assign_coords(time=new_times)

    year_range_args = [year_range[0], year_range[1] + 1]
    annual_stats = [
        get_annual_stats(erads, year, stats)
        for year in tqdm(list(range(*year_range_args)), 'Annual Stats')
    ]

    combined_stats = xr.merge(annual_stats, combine_attrs='drop_conflicts')

    print(combined_stats)

    write_job = combined_stats.chunk(chunks).to_zarr(
        outputfile, mode='w', compute=False, consolidated=True
    )

    with ProgressBar():
        write_job.persist()


if __name__ == '__main__':
    main()
