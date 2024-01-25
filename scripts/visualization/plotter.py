from matplotlib import pyplot as plt
import matplotlib
import numpy as np
import os
import pandas as pd
import yaml
import glob
import seaborn as sns
import scipy
from math import floor, ceil
import argparse

WARMUP_PERCENTAGE = 0.1
COOLDOWN_PERCENTAGE = 0.1
plot_types = ['lr_overheads', 'cl_overheads', 'lr_static', 'ri_static', 'cl_static', 'lr_dynamic', 'ri_dynamic',
              'lr_dynamic_odroid_cluster', 'ri_dynamic_odroid_cluster', 'synthetic']

# Create an ArgumentParser instance
parser = argparse.ArgumentParser(description='Description of your program')

# Argument for the "plot" option with choices
parser.add_argument(
    'plot',
    choices=plot_types,
    help=f'Choose the type of plot: {plot_types}'
)


# Argument for the "target_folder" option with type 'str' and a custom type check function
def is_valid_folder(path):
    if not os.path.exists(path) or not os.path.isdir(path):
        raise argparse.ArgumentTypeError(f"'{path}' is not a valid folder.")
    return path


parser.add_argument(
    'target_folder',
    type=is_valid_folder,
    help='Specify the target folder for the operation'
)

parser.add_argument(
    '--show-in-popup',
    action='store_true',
    help='Enable to show the plot in a popup window (default is False)'
)


def box_info_logging(text):
    text_length = len(text)
    box_width = text_length + 4  # 2 "+" symbols on each side

    print("+" + (box_width - 2) * "-" + "+")
    print(f"| {text} |")
    print("+" + (box_width - 2) * "-" + "+")


def load_data(folder, do_remove_warmup_cooldown=True, skip_matches=None):
    if skip_matches is None:
        skip_matches = []

    def remove_warmup_cooldown(_df):
        tmax = _df.t.max()
        tmin = _df.t.min()
        warmup = tmin + floor((tmax - tmin) * WARMUP_PERCENTAGE)
        cooldown = tmax - ceil((tmax - tmin) * COOLDOWN_PERCENTAGE)
        _df.loc[(_df.t < warmup) | (_df.t > cooldown), 'value'] = np.nan
        return _df

    def load_config(path):
        with open(path, 'r') as file:
            return yaml.load('\n'.join(file.readlines()), Loader=yaml.CLoader)

    def read_csv(file):
        if not file:
            return pd.DataFrame()
        with open(file, "r") as openf:
            num_cols = len(openf.readline().split(","))
        if num_cols == 2:
            names = ('t', 'value')
        elif num_cols == 3:
            names = ('node', 't', 'value')
        else:
            raise Exception(
                f"Unexpected number of columns in {file}: {num_cols}\n Did something go wrong with the experiment?")
        df = pd.read_csv(file, names=names)
        if do_remove_warmup_cooldown:
            df = remove_warmup_cooldown(df)
        return df

    experiment_config = load_config(folder + '/complete_experiment_config.yaml')
    experiment_name = experiment_config["experiment"]["exp_name"]
    data_frames = []
    for sub_dir in os.listdir(folder):
        if not os.path.isdir(folder + '/' + sub_dir):
            continue
        sub_dir_data_frames = []
        experiment_variant = "-".join(sub_dir.split("-")[:-1])
        experiment_rep = sub_dir.split('-')[-1]
        for data_file in glob.glob(folder + '/' + sub_dir + '/' + '*.csv'):
            matches = [skip_match in data_file for skip_match in skip_matches]
            if not any(matches):
                try:
                    df = read_csv(data_file)
                except Exception as e:
                    print(f'Failed to read {data_file}: {e}\nSkipping subfolder {sub_dir} completely!')
                    #                 sub_dir_data_frames = []
                    #                 break
                    continue
                df['parameter'] = data_file.split('/')[-1].split('.')[0].split("_")[0]
                df['experiment'] = experiment_name
                df['variant'] = experiment_variant
                df['rep'] = int(experiment_rep)
                try:
                    query = "_".join(data_file.split('/')[-1].split('.')[0].split("_")[1:])
                except Exception as e:
                    print(e)
                    query = np.nan
                df['query'] = query
                sub_dir_data_frames.append(df)
        for frame in sub_dir_data_frames:
            data_frames.append(frame)

    return pd.concat(data_frames, sort=False, ignore_index=True)


def wrangle(_df):
    variant_dict = {
        "ananke": "ANK-1",
        "nona": "NONA",
        "ananke_k": "ANK-mD",
        "anankek": "ANK-mD",
        "ananke1-1q": "ANK-1 single",
        "ananke1-2q": "ANK-1 dual",
        "anankek-1q": "ANK-mD single",
        "anankek-2q": "ANK-mD dual"
    }
    _df["variant"] = _df["variant"].map(variant_dict)
    _df.loc[_df['value'] < 0, 'value'] = np.nan
    # convert latency to seconds
    _df.loc[_df['parameter'] == 'latency', 'value'] /= 1e3
    _df.loc[_df['parameter'] == 'deliverylatency', 'value'] /= 1e3
    # use time scale relative to minimum for each repetition
    for r in _df["rep"].unique():
        min_t = _df["t"][_df["rep"] == r].min()
        _df.loc[_df["rep"] == r, "t"] = _df.loc[_df["rep"] == r, "t"] - min_t

    return _df


def percentage_diff(value, reference):
    return 100 * (value - reference) / reference


def get_95ci(x: list):
    # 95 percent confidence interval
    bs = scipy.stats.bootstrap((x,), np.mean, confidence_level=0.95)
    mean = np.mean(x)
    lower_end_ci = mean - bs.confidence_interval.low
    upper_end_ci = bs.confidence_interval.high - mean
    return [lower_end_ci, upper_end_ci]


def plot_static(data_df, output_name, show_popup=False):
    def aggDF(df, parameter):
        df = df[df.parameter == parameter].groupby(["experiment", "variant", "rep", "query"]).aggregate(
            {"value": np.mean})
        return df.reset_index()

    def aggDF_for_stacked(df, parameters):
        query_string = ' | '.join(f'parameter == {repr(p)}' for p in parameters)
        df = df.query(query_string).groupby(["experiment", "variant", "rep", "query", "parameter"]) \
            .aggregate({"value": np.mean}) \
            .groupby(["experiment", "variant", "query", "parameter"]) \
            .aggregate({"value": np.mean})
        df = df.reset_index()
        df = df.drop(columns=[col for col in df.columns if col not in ["value", "parameter", "variant"]])
        for param in parameters:
            df[param] = df["value"][df["parameter"] == param]
        df = df.drop(columns=["parameter", "value"])
        df = df.groupby(["variant"]).aggregate({param: np.sum for param in parameters})
        return df

    def aggDF_for_stacked_errorbar(df, parameters):
        query_string = ' | '.join(f'parameter == {repr(p)}' for p in parameters)
        df = df.query(query_string).groupby(["experiment", "variant", "rep", "query", "parameter"]) \
            .aggregate({"value": np.mean})
        df = df.reset_index()
        df = df.groupby(["experiment", "variant", "query", "rep"]).aggregate({"value": np.sum})
        df = df.reset_index()
        return df

    ORDER = ["ANK-mD", "NONA"]
    COLORS = ['bisque', 'pink']

    # ENSURE PLOT ORDER
    data_df["variant"] = pd.Categorical(data_df["variant"], categories=ORDER, ordered=True)
    data_df = data_df.sort_values(by="variant")

    errorbar_params = {"errcolor": "#808080", "capsize": .05, "errwidth": 1.5}

    figsize = (6.5, 1.75)

    fig, axes = plt.subplots(ncols=4, nrows=1, sharey=False, sharex=True, squeeze=False, figsize=figsize)
    axes = axes.flatten()

    # RATE
    sns.barplot(x='variant', y='value', data=aggDF(data_df, "rate"), ax=axes[0], palette=COLORS, **errorbar_params)

    # LATENCY
    sns.barplot(x='variant', y='value', data=aggDF(data_df, "latency"), ax=axes[1], palette=COLORS, **errorbar_params)

    # MEMORY
    aggDF_for_stacked(data_df, ["memory-org", "memory-kafka"]).plot.bar(
        ax=axes[2],
        stacked=True,
        legend=False,
        width=0.8)
    # --invisible plot for errorbar only
    sns.barplot(x='variant',
                y='value',
                data=aggDF_for_stacked_errorbar(data_df, ["memory-org", "memory-kafka"]),
                ax=axes[2],
                alpha=0,
                **errorbar_params)

    custom_legend_elements = [
        matplotlib.patches.Patch(facecolor='gray', label="SPE"),
        matplotlib.patches.Patch(facecolor="gray", label="Kafka", hatch='/////', edgecolor="white")
    ]

    # CPU
    aggDF_for_stacked(data_df, ["cpu-org", "cpu-kafka"]).plot.bar(
        ax=axes[3],
        stacked=True,
        legend=False,
        width=0.8)
    # --invisible plot for error bar only
    sns.barplot(x='variant',
                y='value',
                data=aggDF_for_stacked_errorbar(data_df, ["cpu-org", "cpu-kafka"]),
                ax=axes[3],
                alpha=0,
                **errorbar_params)

    for ax in axes:
        ax.set_ylabel('')
        ax.set_xlabel('')
    axes[0].set_title('Rate (t/s)', size=12)
    axes[1].set_title('Latency (s)', size=12)
    axes[2].set_title('Memory (MB)', size=12)
    axes[3].set_title('CPU (%)', size=12)
    axes[3].legend(handles=custom_legend_elements, handlelength=1.0, handletextpad=0.5, loc=(0.05,0))

    for i, ax in enumerate(axes):

        x_positions = sorted(set([p.get_x() for p in ax.patches]))
        # sum over all patch heights at the first position to get the reference height
        referenceHeight = max([p.get_height() for p in ax.patches if x_positions.index(p.get_x()) == 0])
        print(f"reference height: {referenceHeight}")

        for p in ax.patches:
            # color the patches according to their x-order
            position_index = x_positions.index(p.get_x())
            p.set_color(COLORS[position_index])
            p.set_linewidth(0)
            if p.get_y() > 0:
                # patch belongs to upper part of stacked plot
                p.set_hatch('/////')
                p.set_edgecolor("white")
                p.set_linewidth(0)
                if position_index == 1:
                    # patch must be labelled with text
                    height = p.get_height() + p.get_y()
                    diff = percentage_diff(height, referenceHeight)
                    ax.text(
                        p.get_x() + p.get_width() * 3 / 4,  # x-position
                        (p.get_height()+p.get_y())*0.95, # y-position
                        f'{diff:+2.1f}%',
                        ha='center', va='top', rotation=90, size=9, family='sans', weight='bold', color='#2f2f2f')
            if position_index == 1 and i in [0, 1]:
                # patch must be labelled with text
                height = p.get_height() + p.get_y()
                diff = percentage_diff(height, referenceHeight)
                ax.text(
                    p.get_x() + p.get_width() * 3 / 4,  # x-position
                    (p.get_height()+p.get_y())*0.95, # y-position
                    f'{diff:+2.1f}%',
                    ha='center', va='top', rotation=90, size=9, family='sans', weight='bold', color='#2f2f2f')

    fig.tight_layout(pad=1.02)
    fig.autofmt_xdate(ha='right', rotation=25)
    if show_popup:
        try:
            plt.show()
        except:
            print(f"Could not display figure in separate window.")
    fig.savefig(f'{target_folder}/{output_name}.pdf', pad_inches=.1, bbox_inches='tight', )
    print(f"Saved figure to {target_folder}/{output_name}.pdf")


def plot_overheads(DATA, output_name, show_popup=False):
    def aggDF(df, parameter):
        df = df[df.parameter == parameter].groupby(["experiment", "variant", "rep", "query"]).aggregate(
            {"value": np.mean})
        return df.reset_index()

    def aggDF_for_stacked(df, parameters):
        query_string = ' | '.join(f'parameter == {repr(p)}' for p in parameters)
        df = df.query(query_string).groupby(["experiment", "variant", "rep", "query", "parameter"]) \
            .aggregate({"value": np.mean}) \
            .groupby(["experiment", "variant", "query", "parameter"]) \
            .aggregate({"value": np.mean})
        df = df.reset_index()
        df = df.drop(columns=[col for col in df.columns if col not in ["value", "parameter", "variant"]])
        for param in parameters:
            df[param] = df["value"][df["parameter"] == param]
        df = df.drop(columns=["parameter", "value"])
        df = df.groupby(["variant"]).aggregate({param: np.sum for param in parameters})
        return df.reset_index()

    def aggDF_for_stacked_errorbar(df, parameters):
        query_string = ' | '.join(f'parameter == {repr(p)}' for p in parameters)
        df = df.query(query_string).groupby(["experiment", "variant", "rep", "query", "parameter"]) \
            .aggregate({"value": np.mean})
        df = df.reset_index()
        df = df.groupby(["experiment", "variant", "query", "rep"]).aggregate({"value": np.sum})
        df = df.reset_index()
        return df

    # BAR PLOT

    ORDER = ["ANK-1 single", "ANK-mD single", "ANK-1 dual", "ANK-mD dual"]
    COLORS = ['paleturquoise', 'bisque', 'paleturquoise', 'bisque']

    errorbar_params = {"errcolor": "#808080", "capsize": .05, "errwidth": 1.5}

    figsize = (6.5, 4.0)

    fig, axes = plt.subplots(ncols=2, nrows=2, sharey=False, sharex=True, squeeze=False, figsize=figsize)
    axes = axes.flatten()

    # ENSURE PLOT ORDER
    DATA["variant"] = pd.Categorical(DATA["variant"], categories=ORDER, ordered=True)
    DATA = DATA.sort_values(by="variant")

    # RATE
    sns.barplot(x='variant', y='value', data=aggDF(DATA, "rate"), ax=axes[0], palette=COLORS, **errorbar_params)

    # LATENCY
    sns.barplot(x='variant', y='value', data=aggDF(DATA, "latency"), ax=axes[1], palette=COLORS, **errorbar_params)

    # MEMORY
    aggDF_for_stacked(DATA, ["memory-org", "memory-kafka"]).plot.bar(
        ax=axes[2],
        stacked=True,
        legend=False,
        width=0.8)
    # --invisible plot for errorbar only
    sns.barplot(x='variant',
                y='value',
                data=aggDF_for_stacked_errorbar(DATA, ["memory-org", "memory-kafka"]),
                ax=axes[2],
                alpha=0,
                **errorbar_params)

    custom_legend_elements = [
        matplotlib.patches.Patch(facecolor='gray', label="SPE"),
        matplotlib.patches.Patch(facecolor="gray", label="Kafka", hatch='/////', edgecolor="white")
    ]

    # CPU
    aggDF_for_stacked(DATA, ["cpu-org", "cpu-kafka"]).plot.bar(
        ax=axes[3],
        stacked=True,
        legend=False,
        width=0.8)
    # --invisible plot for error bar only
    sns.barplot(x='variant',
                y='value',
                data=aggDF_for_stacked_errorbar(DATA, ["cpu-org", "cpu-kafka"]),
                ax=axes[3],
                alpha=0,
                **errorbar_params)

    for ax in axes:
        ax.set_ylabel('')
        ax.set_xlabel('')
    axes[0].set_title('Rate (t/s)', size=12)
    axes[1].set_title('Latency (s)', size=12)
    axes[1].yaxis.tick_right()
    axes[2].set_title('Memory (MB)', size=12)
    axes[3].set_title('CPU (%)', size=12)
    axes[3].yaxis.tick_right()
    axes[3].legend(handles=custom_legend_elements, handlelength=1.0, handletextpad=0.5, loc="best")

    for i, ax in enumerate(axes):
        # plot a separating line
        ylims = ax.get_ylim()
        ax.plot([1.5, 1.5], ylims, color="gray", linewidth=1)
        ax.set_ylim(ylims)

        # get the reference heights for creating percentage change labels
        candidates = [p for p in ax.patches if p.get_y() == 0]
        reference_heights = [candidates[0].get_height()] * 2 + [candidates[2].get_height()] * 2

        x_positions = sorted(set([p.get_x() for p in ax.patches]))
        labelled_positions = set()

        for p in ax.patches:
            # color the patches according to their x-order
            position_index = x_positions.index(p.get_x())
            p.set_color(COLORS[position_index])
            p.set_linewidth(0)
            if p.get_y() > 0:
                # patch belongs to upper part of stacked plot
                p.set_hatch('/////')
                p.set_edgecolor("white")
                p.set_linewidth(0)
                height = p.get_height() + p.get_y()
                diff = percentage_diff(height, reference_heights[position_index])
                if not position_index in labelled_positions:
                    ax.text(
                        p.get_x() + p.get_width() * 3 / 4,  # x-position
                        ax.get_ylim()[1] * 0.1,  # y-position
                        f'{diff:+2.1f}%',
                        ha='center', rotation=90, size=9, family='sans', weight='bold', color='#2f2f2f')
                    labelled_positions.add(position_index)
            if position_index in [1, 3] and i in [0, 1]:
                height = p.get_height() + p.get_y()
                diff = percentage_diff(height, reference_heights[position_index])
                ax.text(
                    p.get_x() + p.get_width() * 3 / 4,  # x-position
                    ax.get_ylim()[1] * 0.1,  # y-position
                    f'{diff:+2.1f}%',
                    ha='center', rotation=90, size=9, family='sans', weight='bold', color='#2f2f2f')
                labelled_positions.add(position_index)

    p_ank1 = axes[0].patches[0]
    p_ankmd = axes[0].patches[1]
    axes[0].text(
        p_ank1.get_x() + p_ank1.get_width() * 1 / 4,  # x-position
        axes[0].get_ylim()[1] * 0.1,  # y-position
        "ANK-1",
        ha='center', rotation=90, size=10, family='sans', weight='bold', color='#2f2f2f', alpha=0.8)
    axes[0].text(
        p_ankmd.get_x() + p_ankmd.get_width() * 1 / 4,  # x-position
        axes[0].get_ylim()[1] * 0.1,  # y-position
        "ANK-mD",
        ha='center', rotation=90, size=10, family='sans', weight='bold', color='#2f2f2f', alpha=0.8)

    axes[0].set_xticks([0.5, 2.5], ["single", "dual"])
    for ax in axes:
        ax.xaxis.set_ticks_position('none')

    fig.tight_layout()
    fig.autofmt_xdate(ha='center', rotation=0)
    fig.subplots_adjust(hspace=0.25)
    if show_popup:
        try:
            plt.show()
        except:
            print(f"Could not display figure in separate window.")
    fig.savefig(f'{target_folder}/{output_name}.pdf', pad_inches=.1, bbox_inches='tight', )
    print(f"Saved figure to {target_folder}/{output_name}.pdf")


def plot_dynamic(folder, output_name, show_popup=False):
    flink_durations = {}
    removal_ends = {}
    removal_starts = {}
    addition_ends = {}
    addition_starts = {}

    def get_95ci(x: list):
        # 95 percent confidence interval
        bs = scipy.stats.bootstrap((x,), np.mean, confidence_level=0.95)
        mean = np.mean(x)
        lower_end_ci = mean - bs.confidence_interval.low
        upper_end_ci = bs.confidence_interval.high - mean
        return [lower_end_ci, upper_end_ci]

    for sub_dir in os.listdir(folder):
        if not os.path.isdir(folder + '/' + sub_dir):
            continue
        _, experiment_rep = sub_dir.split('-')
        experiment_rep = int(experiment_rep)
        for data_file in glob.glob(folder + '/' + sub_dir + '/' + '*.csv'):
            if "flink_deployment_log.csv" in data_file:
                flink_durations[experiment_rep] = []
                with open(data_file, "r") as openf:
                    start = 0
                    for line in openf:
                        if "flink-start-deploy" in line:
                            start = int(line.split(",")[1])
                        if "flink-end-deploy" in line:
                            end = int(line.split(",")[1])
                            flink_durations[experiment_rep].append(end - start)
            if "timelogger_NONA" in data_file:
                removal_ends[experiment_rep] = []
                with open(data_file, "r") as openf:
                    for line in openf:
                        removal_ends[experiment_rep].append(int(line.split(",")[0]))
            if "timelogger_bash" in data_file:
                removal_starts[experiment_rep] = []
                addition_ends[experiment_rep] = []
                addition_starts[experiment_rep] = []
                with open(data_file, "r") as openf:
                    for line in openf:
                        value = int(line.split(",")[1])
                        if "addition-start" in line:
                            addition_starts[experiment_rep].append(value)
                        if "addition-end" in line:
                            addition_ends[experiment_rep].append(value)
                        if "removal-start" in line:
                            removal_starts[experiment_rep].append(value)
    removal_durations = {}
    addition_durations = {}

    for rep in range(1, len(addition_ends.keys()) + 1):
        removal_durations[rep] = []
        for start, end in zip(removal_starts[rep], removal_ends[rep]):
            removal_durations[rep].append(end - start)
        addition_durations[rep] = []
        for start, end in zip(addition_starts[rep], addition_ends[rep]):
            addition_durations[rep].append(end - start)

    figsize = (6.5, 3.0)

    C1 = "pink"
    C2 = "lightsteelblue"

    fig, axes = plt.subplots(ncols=1, nrows=2, sharey=False, sharex=False, squeeze=False, figsize=figsize)
    axes = axes.flatten()

    addition_averages = []
    addition_stds = []
    flink_averages = []
    removal_averages = []
    removal_stds = []
    for transition in range(len(addition_durations[1])):
        adds = [addition_durations[rep][transition] for rep in range(1, len(addition_ends.keys()) + 1)]
        rems = [removal_durations[rep][transition] for rep in range(1, len(addition_ends.keys()) + 1)]
        flinks = [flink_durations[rep][transition] for rep in range(1, len(addition_ends.keys()) + 1)]
        addition_averages.append(np.mean(adds))
        addition_stds.append(get_95ci(adds))
        removal_averages.append(np.mean(rems))
        removal_stds.append(get_95ci(rems))
        flink_averages.append(np.mean(flinks))

    addition_stds = np.array(addition_stds).T
    removal_stds = np.array(removal_stds).T

    x_tick_positions = list(range(1, 1 + len(addition_durations[1])))

    addition_labels = [f"+Q{i}" for i in x_tick_positions]
    axes[0].bar(x_tick_positions, flink_averages, color=C2, zorder=2)
    for p, a, f in zip(axes[0].patches, addition_averages, flink_averages):
        head_length = 500
        text_x_pos = p.get_x() + p.get_width() * 0.85
        text_y_pos = p.get_height() * 0.80
        arrow_y_start = text_y_pos * 1.01
        arrow_y_end   = (a + f) / 2 - head_length
        axes[0].text(
            text_x_pos,  # x-position
            text_y_pos,  # y-position
            f'{int(a - f)}ms/{int((a - f) / a * 100)}%',
            horizontalalignment='center', rotation=90, size=9, family='sans', weight='bold', color='#2f2f2f',
            verticalalignment="top")
        axes[0].arrow(text_x_pos, arrow_y_start, 0, arrow_y_end - arrow_y_start, head_width=0.1,
                      head_length=head_length, fc='red', ec='red', zorder=100)
    axes[0].bar(x_tick_positions, addition_averages, color=C1, zorder=1)
    axes[0].errorbar(x_tick_positions, addition_averages, yerr=addition_stds, zorder=10,
                     fmt="none",
                     ecolor="#808080",
                     elinewidth=1.5,
                     capsize=2.5,
                     capthick=1.5)
    axes[0].set_xticks(x_tick_positions)
    axes[0].set_xticklabels(addition_labels)

    removal_labels = [f"-Q{i}" for i in x_tick_positions]
    axes[1].bar(x_tick_positions, removal_averages, color=C1, label="NONA")
    axes[1].errorbar(x_tick_positions, removal_averages, yerr=removal_stds, zorder=10,
                     fmt="none",
                     ecolor="#808080",
                     elinewidth=1.5,
                     capsize=2.5,
                     capthick=1.5)
    axes[1].bar(x_tick_positions, [0] * len(addition_labels), color=C2, zorder=2, label="SPE")
    axes[1].set_xticks(x_tick_positions)
    axes[1].set_xticklabels(removal_labels)

    axes[0].text(1.005, 0.5, "Additions", transform=axes[0].transAxes, fontsize=12, rotation=90,
                 verticalalignment='center', horizontalalignment="right",
                 bbox={"boxstyle": 'round',
                       "facecolor": 'whitesmoke',
                       "edgecolor": "grey"}
                 )
    axes[1].text(1.005, 0.5, "Removals", transform=axes[1].transAxes, fontsize=12, rotation=90,
                 verticalalignment='center', horizontalalignment="right",
                 bbox={"boxstyle": 'round',
                       "facecolor": 'whitesmoke',
                       "edgecolor": "grey"}
                 )
    axes[1].legend(ncol=2, loc="lower center")
    fig.tight_layout()
    fig.subplots_adjust(hspace=0.25)
    if show_popup:
        try:
            plt.show()
        except:
            print(f"Could not display figure in separate window.")
    fig.savefig(f'{target_folder}/{output_name}.pdf', pad_inches=.2, bbox_inches='tight', )
    box_info_logging(f"Saved figure to {target_folder}/{output_name}.pdf")


def plot_synthetic(folder, show_popup=False):
    removal_averages_per_rate = dict()
    removal_stds_per_rate = dict()
    variants = set()

    max_dev = 2

    def confidence_interval_95_and_mean_without_outliers(x: list, max_stds_deviation_from_mean):
        import copy
        y = copy.deepcopy(x)
        raw_std = np.std(y)
        raw_mean = np.mean(y)
        for yi in y:
            if np.abs(yi - raw_mean) >= max_stds_deviation_from_mean * raw_std:
                y.remove(yi)
        adjusted_std = np.std(y)
        adjusted_mean = np.mean(y)

        return adjusted_mean, adjusted_std

    for sub_dir in os.listdir(folder):
        if not os.path.isdir(folder + '/' + sub_dir):
            continue
        variant, _ = sub_dir.split('-')
        variants.add(variant)

    for variant in variants:
        flink_durations = {}
        removal_ends = {}
        removal_starts = {}
        addition_ends = {}
        addition_starts = {}

        for sub_dir in os.listdir(folder):
            if sub_dir.split("-")[0] == variant:
                experiment_rep = int(sub_dir.split("-")[1])
                for data_file in glob.glob(folder + '/' + sub_dir + '/' + '*.csv'):
                    if "flink_deployment_log.csv" in data_file:
                        flink_durations[experiment_rep] = []
                        with open(data_file, "r") as openf:
                            start = 0
                            for line in openf:
                                if "flink-start-deploy" in line:
                                    start = int(line.split(",")[1])
                                if "flink-end-deploy" in line:
                                    end = int(line.split(",")[1])
                                    flink_durations[experiment_rep].append(end - start)
                    if "timelogger_NONA" in data_file:
                        removal_ends[experiment_rep] = []
                        with open(data_file, "r") as openf:
                            for line in openf:
                                removal_ends[experiment_rep].append(int(line.split(",")[0]))
                    if "timelogger_bash" in data_file:
                        removal_starts[experiment_rep] = []
                        addition_ends[experiment_rep] = []
                        addition_starts[experiment_rep] = []
                        with open(data_file, "r") as openf:
                            for line in openf:
                                value = int(line.split(",")[1])
                                if "addition-start" in line:
                                    addition_starts[experiment_rep].append(value)
                                if "addition-end" in line:
                                    addition_ends[experiment_rep].append(value)
                                if "removal-start" in line:
                                    removal_starts[experiment_rep].append(value)

        removal_durations = {}

        for rep in range(1, len(addition_ends.keys()) + 1):
            removal_durations[rep] = []
            for start, end in zip(removal_starts[rep], removal_ends[rep]):
                removal_durations[rep].append(end - start)

        removal_averages = []
        removal_stds = []
        reps = list(range(1, len(addition_ends.keys()) + 1))
        for transition in range(len(removal_durations[1])):
            removal_averages.append(
                confidence_interval_95_and_mean_without_outliers([removal_durations[rep][transition] for rep in reps],
                                                                 max_dev)[0])
            removal_stds.append(
                confidence_interval_95_and_mean_without_outliers([removal_durations[rep][transition] for rep in reps],
                                                                 max_dev)[1])
        removal_averages_per_rate[variant] = removal_averages
        removal_stds_per_rate[variant] = removal_stds

    figsize = (6.5, 3.0)

    C1 = "lightcoral"
    C2 = "lightsteelblue"

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=figsize, sharex=True)

    output_name = "removal_times_synth"

    x_tick_positions = list(range(1, 1 + len(removal_durations[1])))
    removal_labels = [f"-Q{i}" for i in x_tick_positions]

    markers = ["x", "^", "s"]
    colors = ["lightcoral", "indianred", "brown"]

    for i, v in enumerate(sorted(list(variants))):
        ax1.plot(np.array(x_tick_positions) + i / 5 - 1 / 5, removal_averages_per_rate[v],
                 label=f"{v.replace('synthetic', '')}t/s", zorder=2,
                 marker=markers[i],
                 linestyle="",
                 color=colors[i])
        ax1.errorbar(np.array(x_tick_positions) + i / 5 - 1 / 5, removal_averages_per_rate[v],
                     yerr=removal_stds_per_rate[v], zorder=1,
                     fmt="none",
                     ecolor="#808080",
                     elinewidth=1.5,
                     capsize=2.5,
                     capthick=1.5)
        ax2.plot(np.array(x_tick_positions) + i / 5 - 1 / 5, removal_averages_per_rate[v],
                 label=f"{v.replace('synthetic', '')}t/s", zorder=2,
                 marker=markers[i],
                 linestyle="",
                 color=colors[i])
        ax2.errorbar(np.array(x_tick_positions) + i / 5 - 1 / 5, removal_averages_per_rate[v],
                     yerr=removal_stds_per_rate[v], zorder=1,
                     fmt="none",
                     ecolor="#808080",
                     elinewidth=1.5,
                     capsize=2.5,
                     capthick=1.5)
    ax1.set_xticks(x_tick_positions)
    ax1.set_xticklabels(removal_labels)

    # hide the spines between the axes
    min_upper_y = 1400
    max_upper_y = 14000
    min_lower_y = 0
    max_lower_y = 1000

    ax1.spines.bottom.set_visible(False)
    ax2.spines.top.set_visible(False)
    ax1.xaxis.set_visible(False)
    # ax1.tick_params(labeltop=False)  # don't put tick labels at the top
    ax2.xaxis.tick_bottom()

    # "cut line" markers
    d = .5  # proportion of vertical to horizontal extent of the slanted line
    kwargs = dict(marker=[(-1, -d), (1, d)], markersize=12,
                  linestyle="none", color='k', mec='k', mew=1, clip_on=False)
    ax1.plot([0, 1], [0, 0], transform=ax1.transAxes, **kwargs)
    ax2.plot([0, 1], [1, 1], transform=ax2.transAxes, **kwargs)

    # ax1.set_title("Removal durations (ms)")
    ax2.legend(ncol=3, loc='upper center', bbox_to_anchor=(0.5, -0.2))

    for xtick in x_tick_positions:
        if xtick % 2 == 0:
            ax2.fill_between([xtick - 1 / 2, xtick + 1 / 2], min_lower_y, max_lower_y, color='thistle', alpha=0.15,
                             edgecolor="none",
                             zorder=0)
            ax1.fill_between([xtick - 1 / 2, xtick + 1 / 2], min_upper_y, max_upper_y, color='thistle', alpha=0.15,
                             edgecolor="none",
                             zorder=0)
    ax1.set_xlim([x_tick_positions[0] - 1 / 2, x_tick_positions[-1] + 1 / 2])
    ax1.set_ylim(min_upper_y, max_upper_y)  # upper part
    ax2.set_ylim(min_lower_y, max_lower_y)  # lower part
    fig.tight_layout()
    fig.subplots_adjust(hspace=0.1)
    if show_popup:
        try:
            plt.show()
        except:
            print(f"Could not display figure in separate window.")
    fig.savefig(f'{target_folder}/{output_name}.pdf', pad_inches=.2, bbox_inches='tight', )
    box_info_logging(f"Saved figure to {target_folder}/{output_name}.pdf")


if __name__ == '__main__':
    args = parser.parse_args()
    plot_type = args.plot
    target_folder = args.target_folder
    show_in_popup_window = args.show_in_popup

    print(f"Processing data in [{target_folder}]...")
    print()

    if plot_type == 'lr_overheads':
        data = wrangle(load_data(target_folder, skip_matches=["flink_", "timelogger"]))
        plot_overheads(data, "LR_overheads_figure", show_popup=show_in_popup_window)

    elif plot_type == 'cl_overheads':
        data = wrangle(load_data(target_folder, skip_matches=["flink_", "timelogger"]))
        plot_overheads(data, "CL_overheads_figure", show_popup=show_in_popup_window)

    elif plot_type == 'lr_static':
        data = wrangle(load_data(target_folder, skip_matches=["flink_", "timelogger"]))
        plot_static(data, "LR_static_figure", show_popup=show_in_popup_window)

    elif plot_type == 'ri_static':
        data = wrangle(load_data(target_folder, skip_matches=["flink_", "timelogger"]))
        plot_static(data, "RI_static_figure", show_popup=show_in_popup_window)

    elif plot_type == "cl_static":
        data = wrangle(load_data(target_folder, skip_matches=["flink_", "timelogger"]))
        plot_static(data, "CL_static_figure", show_popup=show_in_popup_window)

    elif plot_type == "lr_dynamic":
        plot_dynamic(target_folder, "transition_times_lr_figure", show_popup=show_in_popup_window)

    elif plot_type == "ri_dynamic":
        plot_dynamic(target_folder, "transition_times_ri_figure", show_popup=show_in_popup_window)

    elif plot_type == "lr_dynamic_odroid_cluster":
        plot_dynamic(target_folder, "transition_times_lr_odroid_cluster_figure", show_popup=show_in_popup_window)

    elif plot_type == "ri_dynamic_odroid_cluster":
        plot_dynamic(target_folder, "transition_times_ri_odroid_cluster_figure", show_popup=show_in_popup_window)

    elif plot_type == "synthetic":
        plot_synthetic(target_folder, show_popup=show_in_popup_window)
