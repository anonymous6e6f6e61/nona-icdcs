import subprocess
import re
import argparse


def get_jvm_thread_pids(jvm_pid):
    thread_pids = {}
    thread_dump = subprocess.check_output(['jstack', str(jvm_pid)], encoding='UTF-8', stderr=subprocess.STDOUT)
    for line in thread_dump.split('\n'):
        m = re.match(r'\s*"(.+)".+nid=0x([0-9a-f]+).+', line)
        if m:
            thread_name = m.group(1)
            thread_pid = int(m.group(2), 16)
            thread_pids[thread_pid] = thread_name
    return thread_pids


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get the native PIDs of the threads of a JVM')
    parser.add_argument('JVM_PID', type=int, help='The PID of the JVM to be inspected')
    args = parser.parse_args()

    try:
        thread_pids = get_jvm_thread_pids(args.JVM_PID)
    except subprocess.CalledProcessError as e:
        print('Failed to retrieve PIDs! Did you specifcy the correct JVM PID?')
        print(e.output, end='')
        exit(1)

    for pid, thread_name in thread_pids.items():
        print('{},{}'.format(thread_name, pid))
