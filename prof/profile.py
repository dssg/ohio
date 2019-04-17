import argparse
import datetime
import itertools
import multiprocessing
import pathlib
import random
import re

import matplotlib.pyplot as plt
import numpy
import pandas
import yaml

import ohio.ext.pandas  # noqa

from .tool import (
    free,
    handle_method,
    loadconfig,
    profiler,
    report,
    report_input,
    report_tag,
    report_trial,
    results,
    save_child_results,
)

# profiler submodules
import prof.profilers.copy_from  # noqa
import prof.profilers.copy_to  # noqa


PROFILE_DIMS = ('memory', 'time')
PROFILE_UNITS = ('mb', 's')
PROFILE_LABELS = tuple(
    f'{dim} ({unit})' for (dim, unit) in zip(PROFILE_DIMS, PROFILE_UNITS)
)


def main(prog=None, argv=None):
    start_datetime = datetime.datetime.now()

    parser = argparse.ArgumentParser(prog=prog,
                                     description="profile the codebase")

    parser.add_argument('-r', '--random', action='store_true',
                        help="randomize profiler ordering")
    parser.add_argument('-f', '--filter', action='append', dest='filters',
                        metavar='FILTER', type=re.compile,
                        help="select only profilers matching regular expression(s)")
    parser.add_argument('-t', '--tag-filter', action='append', dest='tag_filters',
                        metavar='FILTER', type=re.compile,
                        help="select only profilers with tags matching regular expression(s)")

    exec_group = parser.add_argument_group("execution arguments")
    exec_group.add_argument('-c', '--count', type=int, default=1,
                            help="number of trials (default: 1)")
    exec_group.add_argument('--table', default='profiling', dest='table_name',
                            help="name to give to table in temporary database")
    exec_group.add_argument('-p', '--plot', default='profile', metavar='PLOT_NAME',
                            help="plot results to path with file name derived from "
                                 "base directory and/or name specified (default: profile)")
    exec_group.add_argument('-np', '--no-plot', action='store_false', dest='plot',
                            help="do not plot results")
    exec_group.add_argument('--subprocess', action='store_true', default=True,
                            help="execute each trial run in a new child process (the default)")
    exec_group.add_argument('--no-subprocess', action='store_false', dest='subprocess',
                            help="execute all runs in the same (parent) process")

    p_required = parser.add_mutually_exclusive_group(required=True)
    p_required.add_argument('-l', '--list',
                            action='store_false', default=True, dest='execute',
                            help="list profilers without executing")
    p_required.add_argument('data_path', type=pathlib.Path, nargs='?',
                            help="path to csv data file to load as input")

    args = parser.parse_args(argv)

    loadconfig.set(args)

    tagged_profilers = profiler.filtered()

    if args.execute:
        report_input()
        print()

    trial_num = args.count if args.execute else 1

    for trial_count in range(trial_num):
        if trial_num > 1:
            if trial_count > 0:
                print()

            report_trial(trial_count)
            print()

        for (tag_index, (tag, profilers)) in enumerate(tagged_profilers.items()):
            if tag_index > 0:
                print()

            if tag is None:
                if tag_index > 0:
                    # only report untagged if any *are* tagged
                    report_tag("<untagged>")
                    print()
            else:
                report_tag(tag)
                print()

            if args.random:
                random.shuffle(profilers)

            for (index, method) in enumerate(profilers):
                if args.execute and index > 0:
                    print()
                    free()
                    print()

                desc = method.__doc__
                if args.execute:
                    desc = 'begin: ' + desc if desc else 'begin ...'

                report(method, desc)

                if args.execute:
                    if args.subprocess:
                        shared_results = multiprocessing.Array('f', len(PROFILE_DIMS))

                        proc = multiprocessing.Process(
                            target=handle_method,
                            args=(method, shared_results, PROFILE_DIMS),
                        )
                        proc.start()
                        proc.join()

                        save_child_results(method, shared_results, PROFILE_DIMS)
                    else:
                        method()

    if args.plot and args.execute:
        for (tag, profilers) in tagged_profilers.items():
            tag_results = {
                # retrieve results for profilers under this tag
                # (converting results defaultdict to dict --
                # for safety and s.t. the data YAML is cleaner)
                profiler.__name__: dict(results.get(profiler))
                for profiler in profilers
            }

            # NOTE: MultiIndex would perhaps be better
            df = pandas.DataFrame(tag_results).transpose()
            df_means = df.applymap(numpy.mean)
            df_stds = df.applymap(numpy.std)

            axis = None
            plot_colors = 'bgrcmyk'
            plot_fmts = 'ov^<>1234spP*+xXDd|_'
            for (index, (label, color, symbol)) in enumerate(zip(
                df.index,
                itertools.cycle(plot_colors),
                itertools.cycle(plot_fmts),
            )):
                data_slice = df_means[index:(index + 1)]
                error_slice = df_stds[index:(index + 1)]
                axis = data_slice.plot.scatter(
                    x=PROFILE_DIMS[0],
                    y=PROFILE_DIMS[1],
                    xerr=error_slice[PROFILE_DIMS[0]],
                    yerr=error_slice[PROFILE_DIMS[1]],
                    ax=axis,
                    label=label,
                    color=color,
                    marker=symbol,
                )

            axis.set(xlabel=PROFILE_LABELS[0], ylabel=PROFILE_LABELS[1])

            plt.title(tag)

            filename_base = pathlib.Path(args.plot)
            if filename_base.is_dir():
                filename_base /= 'profile'  # append default filename base

            tag_slug = tag and tag.replace(' ', '-')[:30]
            tag_tag = f'-{tag_slug}' if tag_slug else ''
            date_suffix = int(start_datetime.timestamp())

            base_filename = f'{filename_base}{tag_tag}-{date_suffix}'
            plot_filename = f'{base_filename}.svg'
            data_filename = f'{base_filename}.yaml'

            plt.savefig(plot_filename)

            print()
            print('[plot]', "saved:", plot_filename)

            with open(data_filename, 'w') as data_fd:
                yaml.dump(tag_results, data_fd)

            print()
            print('[data]', "saved:", data_filename)


if __name__ == '__main__':
    main()
