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
from threading import Thread

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


def load_config(path):
    with open(path, 'r') as file:
        return yaml.load('\n'.join(file.readlines()), Loader=yaml.CLoader)


def execute_quick(command, timeout=10, remote_machine=""):
    if remote_machine != "":
        command = f"ssh {remote_machine} '{command}'"
    return subprocess.run(shlex.split(command), stdout=subprocess.PIPE, timeout=timeout).stdout \
        .decode('utf-8').strip()


def box_info_logging(text):
    text_length = len(text)
    box_width = text_length + 4  # 2 "+" symbols on each side

    logging.info("+" + (box_width - 2) * "-" + "+")
    logging.info(f"| {text} |")
    logging.info("+" + (box_width - 2) * "-" + "+")


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
            process.wait(timeout=60)
        except Exception as e:
            logging.error(
                f"Exception when terminating process [{' '.join(process.args[:2])}...] ({process.pid}): {e}")
    ACTIVE_PROCESSES.clear()


def clean_up(folders, _ssh_prefix="", hard_stop_kafka=False):
    logging.info("CLEAN UP")
    logging.info("-- Terminating active processes")
    stop_active_processes(signal.SIGTERM)
    time.sleep(30)
    for k_instance in list(KAFKA_INSTANCES):
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
            KAFKA_INSTANCES.remove(k_instance)
    for folder in folders:
        if os.path.isdir(folder):
            logging.info(f"-- Removing folder: {folder}")
            shutil.rmtree(folder)


def signal_handler(signum, frame):
    try:
        logging.warning(f"!!! RECEIVED SIGNAL !!!")
        logging.warning("Exiting script after cleaning up")
        hard_stop_kafka = signum == signal.SIGTERM
        clean_up([], _ssh_prefix=ssh_prefix, hard_stop_kafka=hard_stop_kafka)
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
        'experiment_config', help='Path of the experiment configuration yaml file', type=str)
    parser.add_argument(
        "reps", help="Number of repetitions of the experiment", type=int)
    parser.add_argument(
        "duration", help="Duration of experiment in minutes", type=int, default=1)
    parser.add_argument('-nc', '--noCompile', action='store_true', help='If set, the experiment script will not compile'
                                                                        ' the java classes in the beginning')

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

    KAFKA_INSTANCES = set()
    args, unknown_args = get_parser().parse_known_args()

    logging.info(f"Starting run.py")
    logging.info(f"Experiment script: {args.experiment_config}")

    # Load global config and experiment config
    GLOBAL_CONFIG = load_config(f"{ROOT}/configs/global_config.yaml")
    EXPERIMENT_CONFIG = load_config(args.experiment_config)
    exp_name = EXPERIMENT_CONFIG["exp_name"]
    logging.info(f"EXPERIMENT: {exp_name}")
    logging.info("")

    # create results root folder
    COMMIT_HASH = execute_quick("git rev-parse --short HEAD")
    RESULTS_ROOT = f"{ROOT}/results/{exp_name}-{COMMIT_HASH}-{NOW}"
    os.makedirs(RESULTS_ROOT, exist_ok=True)

    flink_runner = "flink_do_run.sh" if EXPERIMENT_CONFIG["device"] == "odroid" else "flink_do_run_server.sh"
    kafka_source_server_port = GLOBAL_CONFIG["kafka_server_port_remote"]
    kafka_sink_server_port = GLOBAL_CONFIG["kafka_server_port"]
    kafka_bin = GLOBAL_CONFIG["kafka_bin"].replace("ROOT", ROOT)
    kafka_conf = GLOBAL_CONFIG["kafka_server_conf"].replace("ROOT", ROOT)
    zookeeper_conf = GLOBAL_CONFIG["zookeeper_conf"].replace("ROOT", ROOT)
    REMOTE_ROOT = GLOBAL_CONFIG["remote_root"]
    ssh_prefix = GLOBAL_CONFIG["ssh_prefix"]
    kafka_bin_remote = GLOBAL_CONFIG["kafka_bin"].replace("ROOT", REMOTE_ROOT)
    kafka_conf_remote = GLOBAL_CONFIG["kafka_server_conf"].replace("ROOT", REMOTE_ROOT)
    zookeeper_conf_remote = GLOBAL_CONFIG["zookeeper_conf"].replace("ROOT", REMOTE_ROOT)
    prov_topic = GLOBAL_CONFIG["provenance_topic"]
    req_topic = GLOBAL_CONFIG["requests_topic"]
    rep_topic = GLOBAL_CONFIG["replies_topic"]
    src_topic = GLOBAL_CONFIG["source_topic"]
    flink_bin = GLOBAL_CONFIG["flink_bin"].replace("ROOT", ROOT)
    marker_filepath = EXPERIMENT_CONFIG["marker_filepath"].replace("ROOT", ROOT)

    duration_seconds = int(args.duration * 60)
    WAIT_EXTRA_SECONDS = 120
    WAIT_BEFORE_SIGKILL_SECONDS = 60

    start_time = time.time()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # verifications
    synthetic_variants = ["synthetic750", "synthetic150", "synthetic15"]
    ananke1_variants = ["ananke1-1q", "ananke1-2q"]
    anankek_variants = ["anankek-1q", "anankek-2q", "anankek"]
    nona_variants = ["nona", "nona_d"]
    legal_variants = nona_variants + ananke1_variants + anankek_variants + synthetic_variants
    legal_devices = ["odroid", "server"]
    used_variants = EXPERIMENT_CONFIG["variants"]
    for variant in used_variants:
        assert variant["name"] in legal_variants, f"Illegal variant in {args.experiment_config}: {variant}. Aborting"
    assert EXPERIMENT_CONFIG["device"] in legal_devices, f"Illegal device: {EXPERIMENT_CONFIG['device']}. Aborting"
    assert " " not in exp_name, f"Illegal experiment name, must not contain spaces! Aborting"

    # save combined config to results root
    full_copied_config = f'{RESULTS_ROOT}/complete_experiment_config.yaml'
    with open(full_copied_config, 'w') as f:
        COMBINED_CONFIG = {"global": GLOBAL_CONFIG, "experiment": EXPERIMENT_CONFIG}
        yaml.dump(COMBINED_CONFIG, f)
    logging.info(f"Copied full experiment configuration to {full_copied_config}")

    logging.info(f"")
    logging.info(f"Repetitions:              {args.reps}")
    logging.info(f"Max duration of each run: {duration_seconds}s")
    logging.info(f"Variants:                 {','.join([variant['name'] for variant in used_variants])}")
    logging.info(f"")

    # # (Re-)Package jar
    if args.noCompile:
        logging.warning("Set to not recompile, skipping JAR compilation step")
    else:
        logging.info(f"(Re-)Packaging project jar file using commit {COMMIT_HASH}")
        compile_jar(ROOT)
    JAR_FROM_ROOT = "target/streamingRetention-1.0.jar"
    JAR = f"{ROOT}/{JAR_FROM_ROOT}"
    REMOTE_JAR = f"{REMOTE_ROOT}/{JAR_FROM_ROOT}"

    run_count = 0

    for variant in used_variants:
        variant_name = variant["name"]

        # set up additional variables if variant is nona_d
        if variant_name == "nona_d":
            query_workers = EXPERIMENT_CONFIG["query_workers"]
            query_workers_ssh_prefixes = []
            query_workers_roots = []
            for query_worker in query_workers:
                query_workers_ssh_prefixes.append(query_worker["ssh_prefix"])
                query_workers_roots.append(query_worker["root"])
        # additional setup end

        kafka_topic_port_dict = dict()
        if variant_name in ananke1_variants:
            kafka_topic_port_dict = {src_topic: kafka_source_server_port}
        elif variant_name in anankek_variants:
            kafka_topic_port_dict = {src_topic: kafka_source_server_port,
                                     prov_topic: kafka_sink_server_port}
        elif variant_name in synthetic_variants:
            kafka_topic_port_dict = {req_topic: kafka_sink_server_port,
                                     rep_topic: kafka_sink_server_port,
                                     prov_topic: kafka_sink_server_port}
        elif variant_name in nona_variants:
            kafka_topic_port_dict = {src_topic: kafka_source_server_port,
                                     req_topic: kafka_sink_server_port,
                                     rep_topic: kafka_sink_server_port,
                                     prov_topic: kafka_sink_server_port}

        for rep in range(1, args.reps + 1):

            for attempt in range(1, MAX_RETRIES + 1):
                success = False
                STAT_DIR = ROOT + "/statFolder"
                RESULT_DIR = f"{RESULTS_ROOT}/{variant_name}-{rep}"

                logging.info("")
                box_info_logging(f"Variant {variant_name} -- Experiment Repetition {rep}/{args.reps} -- "
                                 f"Attempt {attempt}/{MAX_RETRIES}")
                logging.info("")

                try:
                    if os.path.exists(STAT_DIR):
                        shutil.rmtree(STAT_DIR, ignore_errors=True)
                    if os.path.exists(RESULT_DIR):
                        shutil.rmtree(RESULT_DIR, ignore_errors=True)
                    os.mkdir(STAT_DIR)
                    os.mkdir(RESULT_DIR)

                    # init and start kafka
                    if variant_name not in synthetic_variants:
                        logging.info(f"Starting remote Kafka at {kafka_source_server_port}")
                        kafka_utils.stop_start_kafka(kafka_bin_remote,
                                                     kafka_source_server_port,
                                                     zookeeper_conf_remote,
                                                     kafka_conf_remote,
                                                     logging,
                                                     ssh_prefix=ssh_prefix)
                        KAFKA_INSTANCES.add(KafkaInstance(kafka_source_server_port, kafka_bin_remote, True))

                    if variant_name not in ananke1_variants:
                        logging.info(f"Starting local Kafka at {kafka_sink_server_port}")
                        kafka_utils.stop_start_kafka(kafka_bin,
                                                     kafka_sink_server_port,
                                                     zookeeper_conf,
                                                     kafka_conf,
                                                     logging)
                        KAFKA_INSTANCES.add(KafkaInstance(kafka_sink_server_port, kafka_bin, False))

                    kafka_utils.init_kafka_topics(kafka_bin,
                                                  kafka_topic_port_dict,
                                                  logging)

                    # run data source command to feed source_topic
                    if variant_name not in synthetic_variants:
                        if EXPERIMENT_CONFIG["source_remote"] == "true":
                            source_command = " ".join(
                                [f"{ssh_prefix} java -cp {REMOTE_JAR}",
                                 EXPERIMENT_CONFIG['source_class'],
                                 EXPERIMENT_CONFIG['input_file'].replace("ROOT", REMOTE_ROOT),
                                 "localhost:" + kafka_source_server_port.split(":")[-1],
                                 GLOBAL_CONFIG['source_topic']])
                            if "source_extra_args" in EXPERIMENT_CONFIG:
                                source_command += " " + EXPERIMENT_CONFIG["source_extra_args"]
                            logging.info("Data source fed from remote.")
                        else:
                            source_command = " ".join(
                                [f"java -cp {JAR}",
                                 EXPERIMENT_CONFIG['source_class'],
                                 EXPERIMENT_CONFIG['input_file'].replace("ROOT", ROOT),
                                 kafka_source_server_port,
                                 GLOBAL_CONFIG['source_topic']])
                            if "source_extra_args" in EXPERIMENT_CONFIG:
                                source_command += " " + EXPERIMENT_CONFIG["source_extra_args"]
                            logging.info("Data source fed from local.")
                        logging.info(f"Executing source feeder command: \n{source_command}")
                        source_process = execute(source_command)
                        source_loc = "remote" if EXPERIMENT_CONFIG["source_remote"] == "true" else "local"
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
                    if variant_name not in ananke1_variants:
                        execute(f"bash {ROOT}/scripts/utilization-monitoring.sh {STAT_DIR} kafka")
                        logging.info("Kafka and zookeeper utilization logging started")
                        time.sleep(2)
                    # get list of thread names mapped to flink tasks, and monitor all flink-associated threads
                    execute(f"python3 {ROOT}/scripts/flink_threads.py --output_dir {STAT_DIR}")
                    execute(f"python3 {ROOT}/scripts/get_cpu_percentage_per_thread.py "
                            f"--output_dir {STAT_DIR}")
                    logging.info("Flink thread-level monitoring started")

                    # Start Ananke/Nona
                    flink_command = f"{flink_bin}/flink run --class {variant['spe_class']} {JAR} " \
                                    f"{variant['spe_args']} " \
                                    f"--statisticsFolder {STAT_DIR} " \
                                    f"--outputFile output.out " \
                                    f"--kafkaSourceBootstrapServer {kafka_source_server_port} " \
                                    f"--kafkaSinkBootstrapServer {kafka_sink_server_port} " \
                                    f"--markerFilepath {marker_filepath}"
                    logging.info(f"Flink command to be executed by flink runner: \n{flink_command}")
                    main_process = execute(f"bash {flink_runner} '{flink_command}' {duration_seconds} {STAT_DIR}")
                    logging.info(f"Starting flink runner process")
                    monitor(main_process, flink_runner)
                    wait_process = main_process
                    time.sleep(60)
                    sleep_display_remaining_time(30, 1)

                    if variant_name == "nona_d":

                        # Start procedure script
                        logging.info(f"Now starting procedure script for {variant_name}")
                        proc_string = f"bash {ROOT}/experiments/procedure_scripts/{variant['procedure_script']} " \
                                      f"{kafka_sink_server_port} " \
                                      f"{STAT_DIR} " \
                                      f"{kafka_source_server_port} " \
                                      f"{marker_filepath} "
                        for worker_ssh_prefix, worker_root in zip(query_workers_ssh_prefixes, query_workers_roots):
                            proc_string += f"{worker_root} "
                            proc_string += f"{worker_ssh_prefix} "
                            proc_string += f"{JAR_FROM_ROOT.replace('ROOT', worker_root)} "
                            proc_string += f"{GLOBAL_CONFIG['flink_bin'].replace('ROOT', worker_root)} "
                        procedure_process = execute(proc_string)
                        monitor(procedure_process, variant['procedure_script'])
                        wait_process = procedure_process

                    elif variant_name not in ananke1_variants:
                        # Start procedure script
                        logging.info(f"Now starting procedure script for {variant_name}")
                        procedure_process = execute(
                            f"bash {ROOT}/experiments/procedure_scripts/{variant['procedure_script']} "
                            f"{JAR} "
                            f"{kafka_sink_server_port} "
                            f"{STAT_DIR} "
                            f"{flink_bin} "
                            f"{kafka_source_server_port} "
                            f"{marker_filepath}")
                        monitor(procedure_process, variant['procedure_script'])
                        wait_process = procedure_process

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

                    if wait_process != main_process:
                        logging.info(f"Procedure process done: [{' '.join(wait_process.args[:2])}...]")
                        logging.info(f"with exit code: [{exit_code}]")
                        main_process_exit_code = main_process.wait(60)
                        logging.info(f"Exit code of [{' '.join(main_process.args[:2])}...]: [{main_process_exit_code}]")
                        if main_process_exit_code != 0:
                            raise Exception("Exit code indicates failure!")

                    # Execution finished successfully or was terminated by this script
                    if exit_code == 0 or exit_code == -signal.SIGTERM.value:
                        logging.info("")
                        box_info_logging("REPETITION SUCCESSFUL!")
                        copy_folder_contents(STAT_DIR, RESULT_DIR)
                        logging.info(f"Copied experiment data from {STAT_DIR} to {RESULT_DIR}")
                        success = True

                    else:
                        raise Exception("At least one exit code indicates failure!")

                except Exception:
                    logging.warning("An exception occurred during the experiment execution.", exc_info=True)
                    logging.warning(f"Failed attempt {attempt}/{MAX_RETRIES}.")
                    if attempt == MAX_RETRIES:
                        FAILED_EXPERIMENTS.add(f"Variant: {variant_name} / Rep: {rep}")

                clean_up([STAT_DIR], _ssh_prefix=ssh_prefix, hard_stop_kafka=False)
                logging.info("Cooling down for 60 seconds")
                sleep_display_remaining_time(60, 1)

                if success:
                    break

            run_count += 1
            show_progress_bar(run_count / (args.reps * len(EXPERIMENT_CONFIG["variants"])), start_time)

    clean_up([], _ssh_prefix=ssh_prefix, hard_stop_kafka=False)
    logging.info("Results stored at:")
    logging.info(f"{RESULTS_ROOT}")
    if len(FAILED_EXPERIMENTS) > 0:
        logging.warning(f"Failed experiments: {' ++ '.join(FAILED_EXPERIMENTS)}")
    logging.info("")
    box_info_logging(f"Experiment run complete in {round((time.time() - start_time) / 60)} minutes")
    logging.info("")
    logging.info("                                          ")
    logging.info("  ,--.  ,--. ,-----. ,--.  ,--.  ,---.    ")
    logging.info("  |  ,\'.|  |\'  .-.  \'|  ,\'.|  | /  O  \\   ")
    logging.info("  |  |\' \'  ||  | |  ||  |\' \'  ||  .-.  |  ")
    logging.info("  |  | `   |\'  \'-\'  \'|  | `   ||  | |  |  ")
    logging.info("  `--\'  `--\' `-----\' `--\'  `--\'`--\' `--\'  ")
    logging.info("                                          ")
