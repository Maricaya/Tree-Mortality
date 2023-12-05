#!/usr/bin/env python
import os
import json
import click
import numpy as np
from glob import glob
from pathlib import Path
from collections import defaultdict
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

LABELS = {
    'r2': 'R$^2$',
    'rmse': 'RMSE (trees / acre)',
}

XLIM = {
    'r2': (-1, 1),
    'rmse': (0, None),
}


def get_xlim(sname, xlim):
    return tuple(
        x if s is None else s
        for s, x in zip(XLIM[sname], xlim)
    )


@click.command()
@click.argument('resultfiles', nargs=-1, type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(resultfiles, outputfile):

    stats = ('r2', 'rmse')
    statistics = defaultdict(dict)
    for rf in resultfiles:
        with open(rf, 'r') as f:
            result = json.load(f)
        year = result['year']
        for s in stats:
            statistics[year][s] = result[s]

    figs = []
    for year, stats in sorted(statistics.items()):
        fig = plt.figure(figsize=(11, 6))
        fig.suptitle(year, fontsize=18)
        figs.append(fig)
        for s, sname in enumerate(stats):
            ax = fig.add_subplot(1, len(stats), s + 1)
            ax.hist(stats[sname], facecolor='k', edgecolor='none')
            if sname == 'r2':
                ymin, ymax = ax.get_ylim()
                ax.fill_between(
                    [-1, 0], [ymin, ymin], [ymax, ymax],
                    edgecolor='none', facecolor='red', alpha=0.3
                )
                ax.set_ylim(ymin, ymax)

            ax.set_xlabel(LABELS[sname], fontsize=16)
            ax.set_xlim(*get_xlim(sname, ax.get_xlim()))
            ax.set_yticks([])
            ax.set_ylabel('Count', fontsize=16)

    with PdfPages(outputfile) as pdf:
        for fig in figs:
            pdf.savefig(fig)



if __name__ == '__main__':
    main()
