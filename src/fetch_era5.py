#!/usr/bin/env python
import os
import click
import cdsapi
from tqdm import tqdm
from pathlib import Path
from werkzeug.security import safe_join

from convert_bcm_v8 import load_config


@click.command()
@click.argument('configfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputdir', type=click.Path(
    path_type=Path
))
def main(configfile, outputdir):

    config = load_config(configfile)
    years = list(range(*config['years']))
    out_fmt = config['output_format']
    dataset = config['dataset']
    query_params = config['query_parameters']

    if not os.path.exists(outputdir):
        os.mkdir(outputdir)

    c = cdsapi.Client()

    paths = [
        (year, safe_join(outputdir, out_fmt.format(year=year)))
        for year in years
    ]

    # Only download files that have not yet been fetched
    paths = [
        (y, p) for y, p in paths
        if not os.path.exists(p)
    ]

    for year, outfile in tqdm(paths, 'Fetching Data'):
        query_params['year'] = year
        c.retrieve(dataset, query_params, outfile)


if __name__ == '__main__':
    main()
