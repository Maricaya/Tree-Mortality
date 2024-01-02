#!/usr/bin/env python
import os
import re
import click
import numpy as np
from itertools import product
from pathlib import Path
from collections import defaultdict
from sklearn.metrics import (
    mean_squared_error as mse, r2_score,
    ConfusionMatrixDisplay
)
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
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


def get_parts(fname):
    match = re.match('([A-Z]+)([0-9])-([0-9])', fname)
    assert match is not None
    idx = match.group(1)
    cm = int(match.group(2))
    lag = int(match.group(3))
    return idx, cm, lag


def get_matrices(feature_names, importances):
    mapping = {}
    for f, i in zip(feature_names, importances):
        idx, cm, lag = get_parts(f)
        mapping[idx, cm, lag] = i

    idxs = sorted(set([i for i, _, _ in mapping.keys()]))
    cms = sorted(set([c for _, c, _ in mapping.keys()]))
    lags = sorted(set([l for _, _, l in mapping.keys()]))

    I = np.zeros((len(idxs), len(cms), len(lags)))
    for i, c, l in product(idxs, cms, lags):
        ii = idxs.index(i)
        ci = cms.index(c)
        li = lags.index(l)
        I[ii, ci, li] = mapping[i, c, l]

    return I, idxs, cms, lags


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
    imp = results['importances']
    importances = np.median(imp.reshape((-1, imp.shape[-1])), axis=0)
    fnames = results['feature_names']

    I, idxs, cms, lags = get_matrices(fnames, importances)

    assert cms == lags

    figs = []
    for idx, matrix in zip(idxs, I):

        fig = plt.figure(figsize=(5, 4.5))
        figs.append(fig)
        ax = fig.add_subplot(111)

        disp = ConfusionMatrixDisplay(matrix, display_labels=cms)
        disp.plot(
            ax=ax, cmap='plasma', values_format='.0e', include_values=False,
            im_kw={'norm': LogNorm(vmin=10**-5, vmax=10**-1.5)},
        )

        ax.images[-1].colorbar.set_label('Feature Importance Fraction', fontsize=15)
        ax.set_xlabel('Feature Lag Years', fontsize=15)
        ax.set_ylabel('Feature Cumulative Years', fontsize=15)
        ax.set_title(idx.upper(), fontsize=18)

    with PdfPages(outputfile) as pdf:
        for fig in figs:
            pdf.savefig(fig)


if __name__ == '__main__':
    main()
