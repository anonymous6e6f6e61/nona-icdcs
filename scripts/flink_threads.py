"""Print the names and native PIDs of all Flink TaskManager Threads"""

import subprocess
import re
import time
import java_threads as jt
import requests
from argparse import ArgumentParser


def get_task_manager_pids():
    java_procs = subprocess.check_output('jps', encoding='UTF-8').strip().split('\n')
    task_managers = [proc for proc in java_procs if 'TaskManagerRunner' in proc]
    tm_pids = [int(proc.split()[0]) for proc in task_managers]
    return tm_pids


def get_running_flink_jobs():
    FLINK_ENDPOINT = 'http://localhost:8081'
    jobs = requests.get(f'{FLINK_ENDPOINT}/jobs').json()['jobs']
    return [job['id'] for job in jobs if job['status'] == 'RUNNING']


def get_running_flink_tasks():
    FLINK_ENDPOINT = 'http://localhost:8081'
    running_flink_tasks = {}
    running_jobs = get_running_flink_jobs()
    for job in running_jobs:
        job_info = requests.get(f'{FLINK_ENDPOINT}/jobs/{job}').json()
        vertices = job_info['vertices']
        for vertex in vertices:
            running_flink_tasks[vertex['name'].strip()] = job
    return running_flink_tasks


def is_operator(thread_name, flink_running_tasks_):
    """Heuristic check to distinguish operator threads"""
    for task_name, _ in flink_running_tasks_.items():
        c1 = re.sub(r'\W+', '', task_name)
        c2 = re.sub(r'\W+', '', thread_name)
        if re.search(c1, c2):
            return str(True)
    return str(False)


def get_job_from_threadname(thread_name, flink_running_tasks_):
    for task_name, job in flink_running_tasks_.items():
        c1 = re.sub(r'\W+', '', task_name)
        c2 = re.sub(r'\W+', '', thread_name)
        if re.search(c1, c2):
            return job[:10] if len(job) > 10 else job
    return "none"


def get_parser():
    parser = ArgumentParser()
    parser.add_argument(
        '--output_dir', help='Path where output file should be stored', type=str)
    return parser


if __name__ == '__main__':

    args, unknown_args = get_parser().parse_known_args()
    if args.output_dir is not None:
        output_dir = args.output_dir
    else:
        output_dir = "."

    output_file = output_dir + "/" + "flink_threads_names.txt"

    recorded_thread_names = set()

    with open(output_file, "w") as ft:
        while True:
            tm_pids = get_task_manager_pids()
            flink_running_tasks = {}
            try:
                flink_running_tasks = get_running_flink_tasks()
            except Exception as e:
                time.sleep(5)
                continue

            print(f"Found Flink tasks, recording names now.")
            for tm_pid in tm_pids:
                tids = jt.get_jvm_thread_pids(tm_pid)
                is_user = {thread_name: is_operator(thread_name, flink_running_tasks) for thread_name in tids.values()}
                records_written = 0
                for tid, thread_name in sorted(tids.items(), key=lambda entry: is_user[entry[1]], reverse=True):
                    if thread_name not in recorded_thread_names:
                        print(f'{tid},{is_user[thread_name]},{get_job_from_threadname(thread_name, flink_running_tasks)},'
                              f'{thread_name}', file=ft, flush=True)
                        recorded_thread_names.add(thread_name)
                        records_written += 1
                print(f"Recorded {records_written} new Flink thread names.")
            time.sleep(30)
