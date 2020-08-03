#!/usr/local/bin/python3

# Copyright (c) 2020 Stanford University
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""
This file contains code that plots the graphs in the "EPaxos Revisited" paper.
"""

import matplotlib
from matplotlib import lines, patches, ticker
import matplotlib.pyplot as plt
import numpy as np
from os import path

from experiment import Experiment
from results import Results
import utils

# The list of client locations, in the same order as the original EPaxos paper
ORDERED_LOCS = ['va', 'ca', 'or', 'jp', 'eu']
# A color to use when differentiating from the default EPaxos Zipfian experiment
ALTERNATE_ZIPF_COLOR = '#DA70D6' # Light purple
# Fill patterns in a consistent order
HATCHES = [None, '//', '.']

# Set a consistent font for all graphs
matplotlib.rc('font',**{'family':'sans-serif','sans-serif':['Helvetica']})
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['font.family'] = 'Helvetica'

def get_results(root_dirname):
    """
    Returns a list of Results objects for each subdirectory from 'root_dirname'.
    This is typically a set of experiments that will be plotted together on a
    graph.
    """
    return list(map(Results, map(lambda dirname: path.join(root_dirname,
        dirname), utils.subdirectories(root_dirname))))

def is_fixed_epaxos_result(r, perc_conflict):
    """
    Returns True if 'r', a Results object, contains results for an experiment in
    which the protocol was EPaxos and the workload fixed at the percentage
    provided by 'perc_conflict'.
    """
    return r.is_epaxos() and \
        isinstance(r.workload(), Experiment.FixedConflictWorkload) and \
        r.workload().perc_conflict() == perc_conflict

def get_fixed_epaxos_result(results, perc_conflict):
    """
    Returns a Results object from the 'results' list in which the protocol was
    EPaxos and the workload fixed at the percentage provided by 'perc_conflict'.
    """
    return list(filter(lambda r: is_fixed_epaxos_result(r, perc_conflict),
        results))[0]

def is_epaxos_zipf_result(r):
    """
    Returns True if the the Results object 'r' was an experiment for the EPaxos
    protocol and the workload was Zipfian.
    """
    return r.is_epaxos() and isinstance(r.workload(),
        Experiment.ZipfianWorkload)

def get_epaxos_zipf_result(results):
    """
    Returns a Results object from the 'results' list in which the protocol was
    EPaxos and the workload was Zipfian.
    """
    return list(filter(is_epaxos_zipf_result, results))[0]

def get_mpaxos_result(results):
    """
    Returns a Results object from the 'results' list in which the protocol was
    Multi-Paxos.
    """
    return list(filter(lambda r: r.is_mpaxos(), results))[0]

def legend_line(color, linestyle='-'):
    """
    Returns a line with color 'color' and linestyle 'linestyle' that is
    appropriate for use in a legend.
    """
    return lines.Line2D([0], [0], color=color, lw=4, linestyle=linestyle)

def legend_patch_color(color):
    """
    Returns a rectangular swatch with fill color 'color' and a black outline
    that is appropriate for use in a legend.
    """
    return patches.Patch(facecolor=color, edgecolor='black')

def legend_patch_hatch(hatch=None):
    """
    Returns a rectangular swatch that is appropriate for use in a legend.
    'hatch' refers to the fill pattern of the swatch. If there is no hatch, the
    swatch is a solid black rectangle. If there is a hatch, the swatch is a
    white rectangle with black outline and the hatch as the fill pattern.
    """
    return patches.Patch(edgecolor='black',
        facecolor='black' if hatch is None else 'white', hatch=hatch)

def get_color(result):
    """
    Returns the consistent color to associate with a given kind of experiment.
    These colors associate with those used in the original EPaxos evaluation for
    easy comparison.
    """
    if result.batching_enabled(): return ALTERNATE_ZIPF_COLOR
    if result.is_mpaxos(): return '#0085fa' # Blue
    if is_fixed_epaxos_result(result, 0): return '#FFD700' # Yellow
    if is_fixed_epaxos_result(result, 2): return '#FF7F00' # Orange
    if is_fixed_epaxos_result(result, 100): return '#B0171F' # Red
    if result.is_epaxos() and isinstance(result.workload(),
        Experiment.ZipfianWorkload): return 'purple'

def plot_cdf(ax, data, color, linestyle='-'):
    """
    Plots a inverse cumulative distribution of 'data' on the axes 'ax'. The line
    is plotted in the provided 'color' with the provided 'linestyle'.
    """
    n, bins, patches = ax.hist(data, bins=100, density=True, histtype='step',
        cumulative=-1, color=color, zorder=10, linewidth=2, linestyle=linestyle)
    patches[0].set_xy(patches[0].get_xy()[1:])

def format_cdf(ax):
    """
    Styles an axes that contains a cdf plot. Cdfs are logscaled, with markers
    on the y-axis for each power of 10 between .01% and 100%, inclusive. The
    y-axis markers are formatted as percentages. A grey grid is plotted at the
    x and y ticks.
    """
    ax.set_yscale('log')
    ax.set_xlim(xmin=0)
    ax.set_ylim(ymax=1)
    ax.set_yticks([.0001, .001, .01, .1, 1])
    ax.grid(which='major', color='#a3a3a3', linewidth='0.5')

    def to_percent(y, position):
        # Ignore the passed in position. This has the effect of scaling the default
        # tick locations.
        s = str(100 * y)
        if s.endswith('.0'): s = s[:-2]
        if s.startswith('0'): s = s[1:]

        # The percent symbol needs escaping in latex
        if matplotlib.rcParams['text.usetex'] is True:
            return s + r'$\%$'
        else:
            return s + '%'
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(to_percent))

def make_legend(ax, handles, labels, ncol, loc, size=20, squeeze=False):
    """
    Plots and returns a legend on the Axes 'ax'. 'handles' contains the swatches
    for the legend, and 'labels' contains the text to associate with each
    handle. (labels[i] corresponds to the text for handles[i].) 'ncol' refers to
    the number of columns for the legend. 'loc' refers to the location of the
    legend. 'size' refers to the font size for the legend, and handles are
    scaled accordingly. If 'squeeze' is True, the margins between the handles/
    text and the border of the legend are made smaller.
    """
    return ax.legend(handles, labels, facecolor='white',  ncol=ncol, loc=loc,
        columnspacing=1, prop={'size': size},
        borderaxespad=.1 if squeeze else .3, borderpad=.3 if squeeze else .5,
        handletextpad=.5)

def plot_bar_by_loc(ax, expts, yfn, errorfn, colorfn, annotatefn, barwidth,
    fontsize, maxy, ystep, annotationsize=11, annotationheight=10,
    annotationhadjust=None, xlabelhadjust=.1):
    """
    Plots a bar graph on the Axes 'ax'. The bar graph is the same format as
    Figure 4 from the original EPaxos paper; for each location, the results
    from each experiment in 'expts' are side by side, and the locations are
    separated by whitespace. 'yfn' is a function that determines the y values
    that should be plotted given (expt, loc) arguments. The function can
    return any number of y values to plot, which will be plotted consecutively
    behind one another with different hatches, so the returned values should be
    in increasing order. 'errorfn' is an optional function that takes in
    (expt, loc) parameters and returns the y value for an error bar above the
    plotted bar for that experiment and location. 'colorfn' is a function that
    takes in an experiment and returns the color that its bars should be.
    'annotatefn' is a function that takes in an (expt, loc) and returns a string
    to be placed above the topmost bar or error assocated with that experiment
    and location. 'barwidth' refers to the horizontal dimension of the plotted
    bars. 'fontsize' refers to the size of the x and y labels. 'maxy' refers to
    the y value of the top of the graph. 'ystep' refers to the intervals at
    which y markers should be placed. 'annotationsize' refers to the font size
    of annotations. 'annotationheight' refers to how far above the bar the
    annotation should be. 'annotationhadjust' is a function that takes in the
    index of the experiment and returns an amount to move the annotation in
    the horizontal direction. 'xlabelhadjust' refers to the amount the location
    labels should be moved left of center in order for them to appear centered.
    """
    xpos = np.arange(len(ORDERED_LOCS))

    for loci, loc in enumerate(ORDERED_LOCS):
        for expti, expt in enumerate(expts):
            x = xpos[loci] + expti*barwidth

            ys = yfn(expt, loc)
            error = 0

            # Plot each y value in ascending order. The first will be solidly
            # filled, and the remaining will be white with different hatch
            # fillings.
            for yi, y in enumerate(ys):
                color = colorfn(expt) if yi == 0 else 'white'
                edgecolor = 'black' if yi == 0 else colorfn(expt)
                yerr = None
                if yi == len(ys) - 1 and errorfn is not None:
                    error = errorfn(expt, loc)-y
                    yerr = [[0], [error]]
                rect = ax.bar(x, y, barwidth, color=color, edgecolor=edgecolor,
                    hatch=HATCHES[yi], zorder=10-yi, yerr=yerr, capsize=10)

            # Add annotation above the topmost bar if appropriate
            if annotatefn is not None:
                annotation = annotatefn(expt, loc)
                if annotation is not None:
                    x = rect[0].get_x()+rect[0].get_width()/2.
                    if annotationhadjust is not None:
                        x += annotationhadjust(expti)
                    y = rect[0].get_height()+error+annotationheight
                    ax.text(x, y, annotation, ha='center',
                        va='bottom', size=annotationsize, zorder=10)

    # Add labels for locations on the x axis and set font size for y axis
    # labels.
    ax.set_xticks(xpos+barwidth*len(expts)/2-xlabelhadjust)
    ax.set_xticklabels([l.upper() for l in ORDERED_LOCS], fontsize=fontsize)
    ax.tick_params(axis='y', labelsize=fontsize)
    ax.tick_params(axis='x', length=0)

    # Add horizontal lines at specified y intervals.
    ax.set_ylim(0, maxy)
    ys = np.arange(0, maxy+1, ystep)
    ax.set_yticks(ys)
    for y in ys[1:-1]:
        ax.axhline(y=y, color='gray', linestyle='dotted', zorder=1)

def conflict_annotation(expt, loc):
    """
    Returns conflict rate of an experiment as a string percentage. Only does
    this if the experiment is EPaxos, as MPaxos has no conflict rate.
    """
    if expt.is_epaxos():
        conflict_rate = '{}%'.format(round(expt.conflict_rate(loc)*100, 1))
        return conflict_rate

    return None

def reproduction_bar(dirname):
    """
    Generates a graph that reproduces the results of the original EPaxos paper,
    comparing commit and execution latencies, with conflict rate. 'dirname' is a
    directory containing experiments for EPaxos 0%, EPaxos 2%, EPaxos 100%,
    EPaxos Zipf, and MPaxos. The generated graph is saved as an image in the
    directory specified by 'dirname'.
    """
    plt.clf()

    results = get_results(dirname)
    epaxos_0 = get_fixed_epaxos_result(results, 0)
    epaxos_2 = get_fixed_epaxos_result(results, 2)
    epaxos_100 = get_fixed_epaxos_result(results, 100)
    epaxos_zipf = get_epaxos_zipf_result(results)
    mpaxos = get_mpaxos_result(results)
    expts = [epaxos_0, epaxos_2, epaxos_100, epaxos_zipf, mpaxos]

    fig, axs = plt.subplots(2, 1, figsize=(13, 6), constrained_layout=True)
    barwidth = .18
    fontsize = 20
    maxy = 400
    ystep = 100

    # Plot mean commit latency vs. mean execution latency on the top graph
    plot_bar_by_loc(axs[0], expts, lambda expt, loc: (expt.mean_lat_commit(loc),
        expt.mean_lat_exec(loc)), None, get_color, conflict_annotation,
        barwidth, fontsize, maxy, ystep)
    # Plot 99th percentile commit latency vs. 99th percentile execution latency
    # on the bottom graph
    plot_bar_by_loc(axs[1], expts, lambda expt, loc: (expt.p99_lat_commit(loc),
        expt.p99_lat_exec(loc)), None, get_color, conflict_annotation, barwidth,
        fontsize, maxy, ystep)

    axs[0].set_ylabel('Mean Latency (ms)', fontsize=fontsize)
    axs[1].set_ylabel('P99 Latency (ms)', fontsize=fontsize)

    # Add two legends to the top graph: on the left, the legend tells us which
    # colors refer to which experiments. On the right, the legend tells us
    # which fill patterns refer to commit and execution latency.
    leg = make_legend(axs[0], [legend_patch_color(get_color(e)) for e in expts],
        [e.description() for e in expts], ncol=5, loc='upper left')
    make_legend(axs[0], [legend_patch_hatch(HATCHES[i]) for i in range(2)],
        ['Commit', 'Exec'], ncol=2, loc='upper right')
    axs[0].add_artist(leg)

    plt.savefig(path.join(dirname, 'reproduction_bar.pdf'))

def batching_bar(dirname):
    """
    Generates a graph that compares the execution latency between EPaxos with
    batching, EPaxos without batching, and MPaxos. 'dirname' is a directory
    containing those three experiments. The generated graph is saved as an image
    in the directory specified by 'dirname'.
    """
    plt.clf()

    results = get_results(dirname)
    batching = list(filter(lambda r: r.is_epaxos() and r.batching_enabled(),
        results))[0]
    no_batching = list(filter(lambda r: r.is_epaxos() and not r.batching_enabled(),
        results))[0]
    mpaxos = get_mpaxos_result(results)
    expts = [batching, no_batching, mpaxos]

    fig, ax = plt.subplots(figsize=(13, 4), constrained_layout=True)
    barwidth = .25
    fontsize = 22
    maxy = 500
    ystep = 100

    # Plots mean latency as a bar and 99th percentile latency as an error bar
    # above it.
    plot_bar_by_loc(ax, expts, lambda expt, loc: [expt.mean_lat_exec(loc)],
        lambda expt, loc: expt.p99_lat_exec(loc), get_color,
        conflict_annotation, barwidth, fontsize, maxy, ystep, annotationsize=20,
        annotationheight=0.6,
        # Move the conflict rate labels slightly to the left and right so that
        # they are easier to read and can be larger without overlapping.
        annotationhadjust=lambda expti: (-0.05 if expti == 0 else .05),
        xlabelhadjust=.12)

    ax.set_ylabel('Mean/P99 Latency (ms)', fontsize=fontsize)

    make_legend(ax, [legend_patch_color(get_color(e)) for e in expts],
        ['Batching', 'No Batching', mpaxos.description()], ncol=3,
        loc='upper right', size=fontsize)

    plt.savefig(path.join(dirname, 'batching_bar.pdf'))

def commitvexec_cdf(dirname, loc='or'):
    """
    Generates a CDF graph that compares the commit and execution latency of
    EPaxos. The graph contains arrows indicating that the fast path is 1 RTT,
    the slow path (worst case for commit latency) is 2 RTTs, and execution
    latency is bounded, but can be much worse than worst case commit latency.
    'dirname' is a directory containing an EPaxos experiment with Zipfian
    workload. The generated graph is saved as an image in the directory
    specified by 'dirname'. 'loc' specifies which client location's latency
    should be plotted; all client locations will show the same patterns, so we
    only plot one.
    """
    plt.clf()

    expt = get_epaxos_zipf_result(get_results(dirname))

    fig, ax = plt.subplots(figsize=(7, 4), constrained_layout=True)

    # Add CDF lines for commit and exec latency.
    plot_cdf(ax, expt.all_lats_commit(loc), ALTERNATE_ZIPF_COLOR, '--')
    plot_cdf(ax, expt.all_lats_exec(loc), get_color(expt))

    # Add arrows highlighting key latencies.
    make_arrow = lambda text, x: ax.annotate(text, xy=(x, 1), xytext=(x, 10),
        fontsize=18, arrowprops=dict(facecolor='black', arrowstyle='-|>'),
        verticalalignment='top', horizontalalignment='center')
    make_arrow('1 RTT', expt.p50_lat_exec(loc))
    make_arrow('2 RTTs', expt.p99_lat_commit(loc))
    make_arrow('Bound', sorted(expt.all_lats_exec(loc))[-1])

    format_cdf(ax)

    make_legend(ax, [legend_line(get_color(expt)),
        legend_line(ALTERNATE_ZIPF_COLOR, '--')], ['Exec', 'Commit'],
        ncol=1, loc='lower left', size=22, squeeze=True)

    ax.set_xlabel('Latency (ms)', fontsize=20)
    ax.set_ylabel('% Operations', fontsize=20)
    ax.tick_params(axis='both', labelsize=18)

    plt.savefig(path.join(dirname, 'commitvexec_cdf_{}.pdf'.format(loc)))

def infinite_cdf(dirname, loc='eu'):
    """
    Generates a CDF graph that compares the execution latency of EPaxos with and
    without a modification that bounds execution delay. 'dirname' is a directory
    containing two EPaxos experiments with Zipfian workload, one with the fix
    and one without. The generated graph is saved as an image in the directory
    specified by 'dirname'. 'loc' specifies which client location's latency
    should be plotted; all client locations will show the same patterns, so we
    only plot one.
    """
    plt.clf()

    results = get_results(dirname)
    inffix = list(filter(lambda r: is_epaxos_zipf_result(r) and r.inffix(),
        results))[0]
    no_inffix = list(filter(lambda r: is_epaxos_zipf_result(r) and not r.inffix(),
        results))[0]

    fig, ax = plt.subplots(figsize=(5, 4), constrained_layout=True)

    plot_cdf(ax, inffix.all_lats_exec(loc), get_color(inffix))
    plot_cdf(ax, no_inffix.all_lats_exec(loc), ALTERNATE_ZIPF_COLOR, '--')

    format_cdf(ax)

    make_legend(ax, [legend_line(get_color(inffix)),
        legend_line(ALTERNATE_ZIPF_COLOR, '--')], ['Improved', 'Unmodified'],
        ncol=1, loc='upper right', size=22, squeeze=True)

    ax.set_xlabel('Latency (ms)', fontsize=22)
    ax.set_ylabel('% Operations', fontsize=22)
    ax.set_xticks([0, 500, 1000, 1500, 2000])
    ax.tick_params(axis='both', labelsize=22)

    plt.savefig(path.join(dirname, 'infinite_cdf_{}.pdf'.format(loc)))

def infinite_bar(dirname):
    """
    Generates a bar graph that compares the execution latency of EPaxos with and
    without a modification that bounds execution delay. 'dirname' is a directory
    containing two EPaxos experiments with Zipfian workload, one with the fix
    and one without, as well as a Multi-Paxos experiment and EPaxos 0%
    experiment for comparison. The generated graph is saved as an image in the
    directory specified by 'dirname'.
    """
    plt.clf()

    results = get_results(dirname)
    inffix = list(filter(lambda r: is_epaxos_zipf_result(r) and r.inffix(),
        results))[0]
    no_inffix = list(filter(lambda r: is_epaxos_zipf_result(r) \
        and not r.inffix(), results))[0]
    epaxos_0 = get_fixed_epaxos_result(results, 0)
    mpaxos = get_mpaxos_result(results)
    expts = [epaxos_0, inffix, mpaxos]

    fig, axs = plt.subplots(1, 2, figsize=(16, 4), constrained_layout=True)
    barwidth = .25
    fontsize = 20

    # On the left graph, plot mean commit latency vs. mean exec latency vs.
    # mean exec latency without modification.
    def yfn(expt, loc):
        res = [expt.mean_lat_commit(loc), expt.mean_lat_exec(loc)]
        if expt == inffix:
            res.append(no_inffix.mean_lat_exec(loc))
        return res
    plot_bar_by_loc(axs[0], expts, yfn, None, get_color, None, barwidth,
        fontsize, maxy=400, ystep=100)
    # On the right graph, plot 99th percentile commit latency vs. 99th
    # percentile exec latency vs. 99th percentile exec latency without
    # modification.
    def yfn(expt, loc):
        res = [expt.p99_lat_commit(loc), expt.p99_lat_exec(loc)]
        if expt == inffix:
            res.append(no_inffix.p99_lat_exec(loc))
        return res
    plot_bar_by_loc(axs[1], expts, yfn, None, get_color, None, barwidth,
        fontsize, maxy=1500, ystep=250)

    axs[0].set_ylabel('Mean Latency (ms)', fontsize=fontsize)
    axs[1].set_ylabel('P99 Latency (ms)', fontsize=fontsize)

    leg = make_legend(axs[0], [legend_patch_color(get_color(e)) for e in expts],
        [e.description() for e in expts], ncol=1, loc='upper left')
    make_legend(axs[0], [legend_patch_hatch(HATCHES[i]) for i in range(3)],
        ['Commit', 'Exec, Improved', 'Exec'], ncol=2, loc='upper right')
    axs[0].add_artist(leg)

    plt.savefig(path.join(dirname, 'infinite_bar.pdf'))

if __name__ == '__main__':
    """
    Plots all graphs for experiments that have already been run.
    """
    reproduction_bar('results/reproduction')
    batching_bar('results/batching')
    commitvexec_cdf('results/commitvexec')
    infinite_cdf('results/inffix')
    infinite_bar('results/inffix')
