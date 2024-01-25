import psutil
import time
import flink_threads as ft
from argparse import ArgumentParser


def get_parser():
    parser = ArgumentParser()
    parser.add_argument(
        '--output_dir', help='Path where output file should be stored', type=str)
    return parser


def get_threads_cpu_percent(p, interval=1):
    total_percent = p.cpu_percent(interval)
    total_time = sum(p.cpu_times())
    return [(t.id, total_percent * ((t.system_time + t.user_time) / total_time)) for t in p.threads()]


if __name__ == '__main__':

    args, unknown_args = get_parser().parse_known_args()
    if args.output_dir is not None:
        output_dir = args.output_dir
    else:
        output_dir = "."

    output_file = output_dir + "/" + "flink_threads_percentages.txt"

    try:
        with open(output_file, "w") as f:
            found_tms = False
            while True:
                flink_pids = ft.get_task_manager_pids()
                if len(flink_pids) == 0:
                    time.sleep(1)
                    continue
                if not found_tms:
                    print(f"Found Flink TaskManager pids for first time: {flink_pids}")
                    found_tms = True
                procs = [psutil.Process(pid) for pid in flink_pids]
                for proc in procs:
                    for line in get_threads_cpu_percent(proc):
                        print(int(round(time.time() * 1000)), *line, sep=",", file=f, flush=True)
    except Exception as e:
        print(f"An exception occurred: {e}")
