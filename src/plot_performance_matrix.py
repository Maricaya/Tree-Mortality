#!/usr/bin/env python
import os
import json
import click
import numpy as np
from tqdm import tqdm
from pathlib import Path
from collections import defaultdict
from sklearn.metrics import (
    mean_squared_error as mse, r2_score,
    ConfusionMatrixDisplay
)
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


PLOT_KWARGS = {
    'r2': {
        'cmap': 'PiYG',
        'im_kw': dict(vmin=-1, vmax=1),
    },
    'rmse': {
        'cmap': 'plasma',
        'im_kw': dict(vmin=0),
    },
}


def get_matrices(years, metrics):
    all_metrics = set([])
    for k, v in metrics.items():
        all_metrics |= set(v.keys())

    return {
        m: np.array([[
            metrics[yi, yj][m] for yj in years
        ] for yi in years])
        for m in all_metrics
    }


@click.command()
@click.argument('resultfile', type=click.Path(
    path_type=Path, exists=True
))
@click.argument('outputfile', type=click.Path(
    path_type=Path, exists=False
))
def main(resultfile, outputfile):

    metrics = defaultdict(dict)
    results = np.load(resultfile)
    years = results['years']
    targets = results['targets']
    predictions = results['predictions']

    for yi, year_i in tqdm(list(enumerate(years)), 'Calculating metrics'):
        for yj, year_j in enumerate(years):
            pred = predictions[yi, yj, :]
            true = targets[yi, yj, :]
            metrics[year_i, year_j]['r2'] = r2_score(true, pred)
            metrics[year_i, year_j]['rmse'] = mse(true, pred, squared=False)

    matrices = get_matrices(years, metrics)

    figs = []
    for mname, matrix in sorted(matrices.items()):

        fig = plt.figure(figsize=(10, 8))
        figs.append(fig)
        ax = fig.add_subplot(111)

        disp = ConfusionMatrixDisplay(matrix, display_labels=years)
        disp.plot(ax=ax, **PLOT_KWARGS[mname])

        ax.set_xlabel('Testing Year', fontsize=15)
        ax.set_ylabel('Training Year', fontsize=15)
        ax.set_title(mname.upper(), fontsize=18)

    with PdfPages(outputfile) as pdf:
        for fig in figs:
            pdf.savefig(fig)


if __name__ == '__main__':
    main()
