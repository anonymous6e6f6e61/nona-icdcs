import signal
from argparse import ArgumentParser
import yaml
import logging
import os
import shlex
import subprocess
from datetime import datetime
import shutil
import time
import kafka_utils
import sys
from threading import Thread, Lock

ROOT = os.path.abspath("../.")
NOW = datetime.now().strftime("%Y%m%d-%H%M%S")
MAX_RETRIES = 3
ACTIVE_PROCESSES = set()
FAILED_EXPERIMENTS = set()
KAFKA_PORTS = set()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s]: %(message)s",
                    handlers=[logging.StreamHandler(),
                              logging.FileHandler(
                                  f"run.log")
                              ])


class KafkaInstance:
    def __init__(self, _port, _bin, _remote):
        self.port = _port
        self.bin = _bin
        self.remote = _remote

    def string(self):
        return f"KafkaInstance: port {self.port}; bin {self.bin}; remote {self.remote}"


# Constructor method
# Initialize instance variables

def load_config(path):
    with open(path, 'r') as file:
        return yaml.load('\n'.join(file.readlines()), Loader=yaml.CLoader)


def execute_quick(command, timeout=10, remote_machine=""):
    if remote_machine != "":
        command = f"ssh {remote_machine} '{command}'"
    return subprocess.run(shlex.split(command), stdout=subprocess.PIPE, timeout=timeout).stdout \
        .decode('utf-8').strip()


def execute(command):
    process = subprocess.Popen(shlex.split(
        command), stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    ACTIVE_PROCESSES.add(process)
    return process


def monitor(process, name):
    def continuous_output(process):
        for line in process.stdout:
            logging.info(f"[{name}] {line.decode('utf-8').strip()}")

    t = Thread(target=continuous_output, args=(process,))
    t.start()


def stop_active_processes(sig):
    for process in ACTIVE_PROCESSES:
        try:
            process.send_signal(sig)
        except Exception as e:
            logging.error(
                f'Exception when terminating process {process.args[0]} ({process.pid}): {e}')
    ACTIVE_PROCESSES.clear()


def clean_up(folders, _ssh_prefix="", hard_stop_kafka=False):
    logging.info("CLEAN UP")
    logging.info("-- Terminating active processes")
    stop_active_processes(signal.SIGTERM)
    for k_instance in KAFKA_INSTANCES:
        logging.info(f"-- Kafka instance to stop: {k_instance.string()}")
        if not hard_stop_kafka:
            try:
                kafka_utils.stop_kafka(k_instance.bin, k_instance.port, logging,
                                       ssh_prefix=_ssh_prefix if k_instance.remote else "")
                KAFKA_INSTANCES.remove(k_instance)
            except subprocess.TimeoutExpired:
                logging.warning(f"-- Cannot connect to Kafka server {k_instance.port} "
                                f"to retrieve topics to delete them. Trying it the hard way")

                kafka_utils.hard_stop_kafka(k_instance.bin, logging,
                                            ssh_prefix=_ssh_prefix if k_instance.remote else "")
        else:
            kafka_utils.hard_stop_kafka(k_instance.bin, logging,
                                        ssh_prefix=_ssh_prefix if k_instance.remote else "")
    for folder in folders:
        if os.path.isdir(folder):
            logging.info(f"-- Removing folder: {folder}")
            shutil.rmtree(folder)


def signal_handler(signum, frame):
    try:
        logging.warning(f"!!! RECEIVED SIGNAL !!!")
        logging.warning("Exiting script after cleaning up")
        hard_stop_kafka = signum == signal.SIGTERM
        clean_up([], hard_stop_kafka=hard_stop_kafka)
        exit(1)
    except Exception:
        logging.error("Error during clean up! Exiting.")
        exit(1)


def copy_folder_contents(source_folder, dest_folder):
    if not os.path.isdir(source_folder) or not os.path.isdir(dest_folder):
        logging.warning(f"Folder not found: {source_folder} OR {dest_folder}")
    else:
        allfiles = os.listdir(source_folder)
        for f in allfiles:
            src_path = os.path.join(source_folder, f)
            dst_path = os.path.join(dest_folder, f)
            shutil.copy2(src_path, dst_path)
        logging.info(f"Copied all files from {source_folder} to {dest_folder}")


def show_progress_bar(progress, _start_time):
    bar_length = 30  # Modify this to change the length of the progress bar
    status = ""
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = 'error: progress var must be float'
    if progress < 0:
        progress = 0
        status = 'Halt...'
    if progress >= 1:
        progress = 1
        status = 'Done...'
    block = int(round(bar_length * progress))
    text = 'Progress: [{0}] {1:.2f}% {2}'.format(
        '#' * block + '-' * (bar_length - block),
        progress * 100, status)
    logging.info(text)
    # Estimate remaining time
    duration = time.time() - _start_time
    remaining_time = (duration / progress) - duration
    remaining_time = remaining_time / 60  # convert to minutes
    logging.info(
        'Estimated Remaining Time: {:.2f} minutes'.format(remaining_time))


def sleep_display_remaining_time(duration, interval=1):
    for remaining in range(duration, 0, -interval):
        sys.stdout.write("\r")
        sys.stdout.write("{:2d} seconds remaining.".format(remaining))
        sys.stdout.flush()
        time.sleep(interval)
    sys.stdout.write("\r")
    sys.stdout.write("0 seconds remaining.\n")
    sys.stdout.flush()


def get_parser():
    parser = ArgumentParser()
    parser.add_argument(
        "duration", help="Duration of experiment in minutes", type=int, default=1)
    parser.add_argument('-nc', '--noCompile', action='store_true', help='If set, the experiment script will not compile'
                                                                        'the java classes in the beginning')
    parser.add_argument('-nrk', '--noRestartKafka', action='store_true', help='If set, do not restart Kafka')
    parser.add_argument('-jsk', '--justStartKafka', action='store_true', help='Just start Kafka and exit (then, next '
                                                                              'time, run with -nrk option)')

    return parser


def compile_jar(root):
    execute_quick(f"mvn -f {root}/pom.xml clean", timeout=20)
    try:
        shutil.rmtree(f"{root}/target")
        logging.info(f"Folder {root}/target still existed, but was deleted now")
    except FileNotFoundError:
        logging.info(f"Successfully removed folder {root}/target")
    compile_std_out = execute_quick(f"mvn -f {root}/pom.xml package", timeout=120)
    if "[ERROR]" in compile_std_out:
        raise Exception(f"Error when running 'mvn package'. Abort")


if __name__ == '__main__':

    input_file = "ROOT/input/ioal17insfry4naurtybkp44dxev59ta.txt"
    source_class = "streamingRetention.queries.linearRoad.LinearRoadKafkaProducerDebug"

    KAFKA_INSTANCES = []
    args, unknown_args = get_parser().parse_known_args()

    logging.info(f"Starting run-local.py")
    logging.info(f"Used for debugging and testing")

    # Load global config and experiment config
    GLOBAL_CONFIG = load_config(f"{ROOT}/configs/config.yaml")
    exp_name = "LOCALDEBUG"
    logging.info(f"EXPERIMENT: {exp_name}")
    logging.info("")

    # create results root folder
    COMMIT_HASH = execute_quick("git rev-parse --short HEAD")
    RESULTS_ROOT = f"{ROOT}/results/{exp_name}-{COMMIT_HASH}-{NOW}"
    os.makedirs(RESULTS_ROOT, exist_ok=True)

    flink_runner = "flink_do_run_debug_local.sh"
    kafka_source_server_port = GLOBAL_CONFIG["kafka_server_port"]
    kafka_sink_server_port = GLOBAL_CONFIG["kafka_server_port"]
    kafka_bin = GLOBAL_CONFIG["kafka_bin"].replace("ROOT", ROOT)
    kafka_conf = GLOBAL_CONFIG["kafka_server_conf"].replace("ROOT", ROOT)
    zookeeper_conf = GLOBAL_CONFIG["zookeeper_conf"].replace("ROOT", ROOT)
    prov_topic = GLOBAL_CONFIG["provenance_topic"]
    req_topic = GLOBAL_CONFIG["requests_topic"]
    rep_topic = GLOBAL_CONFIG["replies_topic"]
    src_topic = GLOBAL_CONFIG["source_topic"]
    flink_bin = GLOBAL_CONFIG["flink_bin"].replace("ROOT", ROOT)
    marker_filepath = f"{ROOT}/scripts/.marker"

    duration_seconds = int(args.duration * 60)
    WAIT_EXTRA_SECONDS = 120
    WAIT_BEFORE_SIGKILL_SECONDS = 60

    start_time = time.time()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if args.justStartKafka:
        kafka_utils.stop_start_kafka(kafka_bin,
                                     kafka_sink_server_port,
                                     zookeeper_conf,
                                     kafka_conf,
                                     logging)
        logging.info("Just started kafka, nothing else")
        exit(0)

    logging.info(f"")
    logging.info(f"Max duration of run: {duration_seconds}s")
    logging.info(f"")

    # # (Re-)Package jar
    if args.noCompile:
        logging.warning("Set to not recompile, skipping JAR compilation step")
    else:
        logging.info(f"(Re-)Packaging project jar file using commit {COMMIT_HASH}")
        compile_jar(ROOT)
    JAR = f"{ROOT}/target/streamingRetention-1.0.jar"

    run_count = 0

    KAFKA_INSTANCES = []

    kafka_topic_port_dict = {src_topic: kafka_sink_server_port,
                             req_topic: kafka_sink_server_port,
                             rep_topic: kafka_sink_server_port,
                             prov_topic: kafka_sink_server_port}

    for attempt in range(1, MAX_RETRIES + 1):
        success = False
        STAT_DIR = ROOT + "/statFolder"
        RESULT_DIR = f"{RESULTS_ROOT}/debug_local"

        logging.info("")
        logging.info(f">>> ATTEMPT {attempt}/{MAX_RETRIES} <<<")
        logging.info("")

        try:
            if os.path.exists(STAT_DIR):
                shutil.rmtree(STAT_DIR, ignore_errors=True)
            if os.path.exists(RESULT_DIR):
                shutil.rmtree(RESULT_DIR, ignore_errors=True)
            os.mkdir(STAT_DIR)
            os.mkdir(RESULT_DIR)

            if not args.noRestartKafka:
                logging.info(f"Starting local Kafka at {kafka_sink_server_port}")
                kafka_utils.stop_start_kafka(kafka_bin,
                                             kafka_sink_server_port,
                                             zookeeper_conf,
                                             kafka_conf,
                                             logging)
            else:
                logging.info(f"Assuming there is already a Kafka instance running at {kafka_sink_server_port}")
            KAFKA_INSTANCES.append(KafkaInstance(kafka_sink_server_port, kafka_bin, False))

            kafka_utils.init_kafka_topics(kafka_bin,
                                          kafka_topic_port_dict,
                                          logging)

            # run data source command to feed source_topic
            source_command = " ".join(
                [f"java -cp {JAR}",
                 source_class,
                 input_file.replace("ROOT", ROOT),
                 kafka_source_server_port,
                 GLOBAL_CONFIG['source_topic']])
            logging.info("Data source fed from local.")
            logging.info(f"Executing source feeder command: \n{source_command}")
            source_process = execute(source_command)
            source_loc = "local"
            monitor(source_process, f"Source Feeder ({source_loc})")
            source_process_wait = 60 * 60
            source_process_exit_code = 1
            try:
                source_process_exit_code = source_process.wait(source_process_wait)
            except subprocess.TimeoutExpired:
                logging.warning(f"Source process did not terminate in {source_process_wait} seconds.")
                logging.warning(f"Now sending SIGTERM to it...")
                source_process.send_signal(signal.SIGTERM)
                try:
                    source_process.wait(30)
                except subprocess.TimeoutExpired:
                    logging.warning("Unsuccessful. Now sending SIGKILL to it...")
                    source_process.send_signal(signal.SIGKILL)
                    time.sleep(5)

            if source_process_exit_code != 0:
                raise Exception(f"Source process exited with non-zero exit code. "
                                f"Exit code: {source_process_exit_code}")

            # start utilization monitoring for kafka and zookeeper if nona is used
            execute(f"bash {ROOT}/scripts/utilization-monitoring.sh {STAT_DIR} kafka")
            logging.info("Kafka and zookeeper utilization logging started")
            time.sleep(2)

            # Start Ananke/Nona
            flink_command = f"{flink_bin}/flink run --class streamingRetention.Nona {JAR} " \
                            "--queryID NONA --slackStrategy ZERO --queryU -1 --graphEncoder NoOpProvenanceGraphEncoder " \
                            "--serializerActivator LINEAR_ROAD_FULL --timestampConverter secToMillis " \
                            f"--statisticsFolder {STAT_DIR} " \
                            f"--outputFile output.out " \
                            f"--kafkaSourceBootstrapServer {kafka_source_server_port} " \
                            f"--kafkaSinkBootstrapServer {kafka_sink_server_port} " \
                            f"--markerFilepath {marker_filepath}"
            logging.info(f"Executing flink command: {flink_command}")
            main_process = execute(f"bash {flink_runner} '{flink_command}' {duration_seconds} {STAT_DIR}")
            logging.info(f"Starting Nona process")
            monitor(main_process, flink_runner)
            sleep_display_remaining_time(40)

            logging.info(f"Now starting procedure script")
            proc_script = "proc--lr-local-debug.sh"
            nona_procedure_process = execute(
                f"bash {ROOT}/experiments/procedure_scripts/{proc_script} {JAR} {kafka_sink_server_port} "
                f"{STAT_DIR} {flink_bin} {kafka_source_server_port} {marker_filepath}")
            monitor(nona_procedure_process, proc_script)
            wait_process = nona_procedure_process

            try:
                exit_code = wait_process.wait(
                    duration_seconds + WAIT_EXTRA_SECONDS)
            except subprocess.TimeoutExpired:
                logging.warning('Experiment timed out. Forcing exit')
                stop_active_processes(signal.SIGTERM)
                try:
                    exit_code = wait_process.wait(WAIT_BEFORE_SIGKILL_SECONDS)
                except subprocess.TimeoutExpired:
                    logging.error(
                        'Failed to cleanup nicely. Sending SIGKILL to active processes. '
                        'Experiment might be incomplete...')
                    stop_active_processes(signal.SIGKILL)
                    exit_code = 1

            logging.info(f"Exit code of {flink_runner}: {exit_code}")

            # Execution finished successfully or was terminated by this script
            if exit_code == 0 or exit_code == -signal.SIGTERM.value:
                logging.info("SUCCESS!")
                logging.info(f"Copying experiment data from {STAT_DIR} to {RESULT_DIR}")
                copy_folder_contents(STAT_DIR, RESULT_DIR)
                success = True

            else:
                raise Exception("Exit code indicates failure!")

        except Exception:
            logging.warning("An exception occurred during the experiment execution.", exc_info=True)
            logging.warning(f"Failed attempt {attempt}/{MAX_RETRIES}.")
            clean_up([], hard_stop_kafka=False)
        if success:
            break

    if not args.noRestartKafka:
        clean_up([], hard_stop_kafka=False)
    logging.info(f"Results stored at {RESULTS_ROOT}")
    if len(FAILED_EXPERIMENTS) > 0:
        logging.warning(f"Failed experiments: {'; '.join(FAILED_EXPERIMENTS)}")
    logging.info("")
    logging.info(f"===> All experiments complete in {round((time.time() - start_time) / 60)} minutes")