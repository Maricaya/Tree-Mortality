#!/usr/bin/env python
import os
import re
import json
import click
import pyproj
import numpy as np
import xarray as xr
import rasterio as rio
import rioxarray as rxr
from glob import glob
from tqdm import tqdm
from pathlib import Path
from zipfile import ZipFile
from datetime import datetime
from collections import defaultdict


FILE_RE = '([a-z]{3})([0-9]{4}[a-z]{3})'


def load_config(configfile):
    with open(configfile, 'r') as f:
        return json.load(f)


def parse_file_info(vinfo, path):
    base = os.path.splitext(os.path.basename(path))[0]
    match = re.match(FILE_RE, base)
    if match is None:
        raise ValueError(f'Unexpected file name format: "{base}"')

    vname = match.group(1)
    if vname not in vinfo:
        raise ValueError(f'Unknown variable name: "{vname}"')

    info = vinfo[vname]

    date = datetime.strptime(match.group(2), '%Y%b')
    date = np.datetime64(date.strftime('%Y-%m'), 'ns')

    return vname, info, date


def time_concat(datasets):
    return xr.concat(
        datasets,
        dim='time',
        data_vars='all',
        coords='all',
    )


def read_archive(vinfo, zipfile):
    last_vname = None

    months = {}

    zfb = os.path.splitext(os.path.basename(zipfile))[0]
    with ZipFile(zipfile) as zf:
        names = sorted(zf.namelist())
        for name in tqdm(names, f'Extracting {zfb}'):
            with zf.open(name) as ah, rio.MemoryFile(ah) as af:
                vname, date, ds = read_file(vinfo, name, af)

                assert last_vname is None or last_vname == vname
                last_vname = vname

                months[date] = ds

    combined = time_concat(
        [ds for _, ds in sorted(months.items())]
    )

    return vname, min(months.keys()), combined


def read_file(vinfo, ascfilename, ascmemfile):
    vname, info, date = parse_file_info(vinfo, ascfilename)

    with ascmemfile.open() as src:
        rs = np.arange(0, src.height)
        cs = np.arange(0, src.width)
        easting, _ = src.xy(np.zeros(cs.shape), cs)
        _, northing = src.xy(rs, np.zeros(rs.shape))

        assert src.count == 1
        data = src.read(1)

        data[data == src.nodata] = np.nan

    xy_units = {'units': 'm'}

    dataset = xr.Dataset(
        data_vars={
            vname: (
                ['time', 'northing', 'easting'],
                data[np.newaxis, ...],
                info
            )
        },
        coords={
            'time': xr.Variable('time', [date]),
            'northing': xr.Variable('northing', northing, xy_units),
            'easting': xr.Variable('easting', easting, xy_units),
        }
    )

    return vname, date, dataset


@click.command()
@click.argument('datadir', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('configfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(datadir, configfile, outputfile):

    config = load_config(configfile)
    vinfo = config['variables']
    pstr = config['projection']

    zip_files = sorted(glob(os.path.join(datadir, '*.zip')))

    datasets = defaultdict(dict)

    for zf in zip_files:
        vname, start, dataset = read_archive(vinfo, zf)
        datasets[vname][start] = dataset

    all_dates = set([])
    for dd in datasets.values():
        all_dates |= set(dd.keys())

    missing = set([])
    for vname, dd in datasets.items():
        mv = all_dates - set(dd.keys())
        for m in mv:
            missing.add((vname, m))

    if len(missing) > 0:
        print('Missing the following datasets:')
        for v, m in sorted(missing):
            print(f'{v}: {m}')
        raise ValueError('Incomplete Dataset')

    dataset = xr.merge([
        time_concat([d for _, d in sorted(dd.items())])
        for vname, dd in sorted(datasets.items())
    ])

    dataset.rio.write_crs(pstr, inplace=True)

    dataset.to_netcdf(
        outputfile, engine='h5netcdf',
        encoding={
            v: {"compression": "gzip", "compression_opts": 9}
            for v in dataset.data_vars
        },
    )


if __name__ == '__main__':
    main()
