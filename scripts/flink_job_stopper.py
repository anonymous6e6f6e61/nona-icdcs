# credit XXX
import time
import requests
import signal
from argparse import ArgumentParser
from argparse import ArgumentError

BASE_URL = 'http://localhost:8081'


def interrupt_handler(sig, frame):
    global ENABLED
    print('[flink_job_stopper] Exiting job stopper...')
    exit(0)


def get_sum(json_entries, key='value'):
    return sum(float(entry[key]) for entry in json_entries)


def get_running_jobs(url):
    request_url = '{}/jobs'.format(url)
    r = requests.get(request_url).json()
    jobs = r['jobs']
    running = [job['id'] for job in jobs if job['status'] == 'RUNNING']
    return set(running)


def cancel_job(url, job):
    request_url = '{}/jobs/{}'.format(url, job)
    print('[flink_job_stopper] Canceling job {}'.format(job))
    result = requests.patch(request_url).json()
    if result:
        print('[flink_job_stopper] Result: {}'.format(result))


if __name__ == '__main__':
    signal.signal(signal.SIGINT, interrupt_handler)
    signal.signal(signal.SIGTERM, interrupt_handler)
    parser = ArgumentParser()
    parser.add_argument("timeout",
                        help="time to wait until jobs are stopped (seconds)",
                        type=int)
    args = parser.parse_args()
    if args.timeout < 0:
        raise ArgumentError('Timeout must be a positive number')
    time.sleep(args.timeout)
    currentJobs = get_running_jobs(BASE_URL)
    if args.timeout == 0:
        print(f"[flink_job_stopper] Was called to immediately cancel all running flink jobs.")
    else:
        print(f'[flink_job_stopper] Becoming active because timeout expired after {args.timeout} seconds.')
    print(f'[flink_job_stopper] Running jobs found: {currentJobs}')
    for job in currentJobs:
        cancel_job(BASE_URL, job)
