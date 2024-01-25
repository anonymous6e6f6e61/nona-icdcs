import os
import subprocess
import shlex
import logging
import yaml
import time
from argparse import ArgumentParser

active_processes = set()
active_processes_remote = set()


def init_logging(log_name):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s]: %(message)s",
                        handlers=[logging.StreamHandler(),
                                  logging.FileHandler(
                                      f"{log_name}.log")
                                  ])


def load_config(path):
    with open(path, 'r') as file:
        return yaml.load('\n'.join(file.readlines()), Loader=yaml.CLoader)


def execute_quick(command, timeout=10, remote_machine=""):
    # returns the stdout output
    if remote_machine != "":
        command = f"ssh {remote_machine} '{command}'"
    return subprocess.run(shlex.split(command), stdout=subprocess.PIPE, timeout=timeout).stdout \
        .decode('utf-8').strip()


def execute_quick_return_list(command, timeout=10):
    return [b.decode('utf-8') for b in
            subprocess.run(shlex.split(command), stdout=subprocess.PIPE, timeout=timeout).stdout \
                .splitlines()]


def execute(command):
    process = subprocess.Popen(shlex.split(
        command), stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    active_processes.add(process)
    return process


def stop_active_processes(sig):
    for process in active_processes:
        try:
            process.send_signal(sig)
        except Exception as e:
            logging.error(
                f'Exception when terminating process {process.args[0]} ({process.pid}): {e}')
    active_processes.clear()


def find_in_string_list(search_terms: list, input: list) -> list:
    output = []
    for item in input:
        if all([term in item for term in search_terms]):
            output.append(item)
    return output


def is_zookeeper_running():
    ps = execute_quick_return_list("ps ax")
    if len(find_in_string_list(["java", "QuorumPeerMain"], ps)) > 0:
        return True
    else:
        return False


def is_kafka_running():
    ps = execute_quick_return_list("ps ax")
    if len(find_in_string_list(["kafka.Kafka", "java"], ps)) > 0:
        return True
    else:
        return False


def start_flink():
    # FLINK_BIN = config["flink_bin"]
    execute_quick(f"{FLINK_BIN}/start-cluster.sh")
    time.sleep(10)


def stop_flink():
    # FLINK_BIN = config["flink_bin"]
    execute_quick(f"{FLINK_BIN}/stop-cluster.sh")
    time.sleep(5)


def startServices():
    logging.info("++ (re-)Starting Flink Cluster")
    stop_flink()
    start_flink()

    # kill services if they are already running
    if is_zookeeper_running():
        logging.info("--Zookeeper already running, terminating it.")
        execute_quick(f"{KAFKA_BIN}/zookeeper-server-stop.sh")
        time.sleep(10)
    if is_kafka_running():
        logging.info("--Kafka already running, terminating it.")
        execute_quick(f"{KAFKA_BIN}/kafka-server-stop.sh")
        time.sleep(10)

    # start zookeeper
    logging.info("++ attempting to start Zookeeper...")
    attempt = 0
    while not is_zookeeper_running():
        attempt += 1
        logging.info(f"++ attempt {attempt}")
        execute_quick(f"{KAFKA_BIN}/zookeeper-server-start.sh -daemon {ZOOKEEPER_CONF}")
        if attempt == 10:
            logging.error("ERROR: cannot start Zookeeper")
            logging.error(f"Check zookeeper logs at {KAFKA_BIN.replace('bin', 'logs')}")
            logging.error("Possible remedy: delete folder /tmp/zookeeper/version-2")
            exit(1)
        time.sleep(10)
    logging.info(f"++ Started Zookeeper using configuration {ZOOKEEPER_CONF}")

    # start kafka, delete any existing topics
    logging.info("++ attempting to start Kafka...")
    attempt = 0
    while not is_kafka_running():
        attempt += 1
        logging.info(f"++ attempt {attempt}")
        execute_quick(f"{KAFKA_BIN}/kafka-server-start.sh -daemon {KAFKA_SERVER_CONF}")
        if attempt == 10:
            logging.error("ERROR: cannot start Kafka")
            exit(1)
        time.sleep(10)
    logging.info(f"++ Started Kafka using konfiguration {KAFKA_SERVER_CONF}")

    delete_kafka_topics()

    # create kafka topics
    logging.info("++ Instantiating Kafka topics now")
    createKafkaTopics()


def createKafkaTopics():
    logging.info("-- Creating topics:")
    for topic in TOPICS:
        logging.info(f"  --[{topic}]")
        execute_quick(f"{KAFKA_BIN}/kafka-topics.sh --bootstrap-server {KAFKA_SERVER_PORT} --topic {topic} --create")
    running_topics = execute_quick_return_list(
        f"{KAFKA_BIN}/kafka-topics.sh --list --bootstrap-server {KAFKA_SERVER_PORT}")
    logging.info(f"-- Topics now on Kafka: {running_topics}")


def stopServices():
    logging.info("-- Stopping Flink cluster.")
    stop_flink()

    if is_kafka_running():
        logging.info("-- Kafka running, deleting all topics and terminating it.")
        delete_kafka_topics()
        execute_quick(f"{KAFKA_BIN}/kafka-server-stop.sh")
        time.sleep(10)

    if is_zookeeper_running():
        logging.info("-- Zookeeper running, terminating it.")
        execute_quick(f"{KAFKA_BIN}/zookeeper-server-stop.sh")
        time.sleep(10)

    logging.info("-- Stopped all services.")


def get_running_flink_jobs():
    # FLINK_BIN = config["flink_bin"]
    console_output = execute_quick_return_list(f"{FLINK_BIN}/flink list --running")
    job_IDs = []
    for line in console_output:
        if "(RUNNING)" in line:
            job_ID = line.split(" : ")[1].split(" : ")[0]
            job_IDs.append(job_ID)
    return job_IDs


def cancel_flink_job(job_id):
    # FLINK_BIN = config["flink_bin"]
    logging.info(f"-- Attempting to cancel flink job {job_id}...")
    logging.info(execute_quick(f"{FLINK_BIN}/flink cancel {job_id}"))


def delete_kafka_topics():
    # KAFKA_BIN = config["kafka_bin"]

    topics = execute_quick_return_list(f"{KAFKA_BIN}/kafka-topics.sh --list --bootstrap-server {KAFKA_SERVER_PORT}")
    if len(topics) > 0 and len(topics[0]) > 0:
        logging.info(f"  -- found topics: {topics}. Now deleting it/them...")
        for topic in topics:
            execute_quick(
                f"{KAFKA_BIN}/kafka-topics.sh --delete --topic {topic} --bootstrap-server {KAFKA_SERVER_PORT}",
                timeout=10)
            logging.info(f"  -- deleted topic: {topic}")
    else:
        logging.info(f"-- No topics found at Kafka instance via bootstrap server {KAFKA_SERVER_PORT}")


def soft_reset():
    logging.info("-- [1/2] cancelling Flink jobs")
    job_IDs = get_running_flink_jobs()
    if len(job_IDs) == 0:
        logging.info("-- No running Flink jobs found.")
    else:
        for job_id in job_IDs:
            cancel_flink_job(job_id)
    logging.info("-- [2/2] deleting Kafka topics")
    if is_kafka_running():
        delete_kafka_topics()
        createKafkaTopics()
    else:
        logging.error("-- Kafka not running, cannot delete topics.")


def cancel_all_flink_jobs():
    job_IDs = get_running_flink_jobs()
    if len(job_IDs) == 0:
        logging.info("-- No running Flink jobs found.")
    else:
        for job_id in job_IDs:
            cancel_flink_job(job_id)


def get_parser():
    parser = ArgumentParser()
    parser.add_argument(
        'command', help='Choose action.', type=str,
        choices=["start", "stop", "kafka-status", "delete-topics", "soft-reset", "flink-cancel-all", "kafka-reset"])
    return parser


config_file_path = os.path.abspath("../configs/global_config.yaml")
print(f"[startStopServices] Loading configuration file {config_file_path}")

global_config = load_config(config_file_path)
KAFKA_BIN = global_config["kafka_bin"]
FLINK_BIN = global_config["flink_bin"]
ZOOKEEPER_CONF = global_config["zookeeper_conf"]
KAFKA_SERVER_CONF = global_config["kafka_server_conf"]
KAFKA_SERVER_PORT = global_config["kafka_server_port"]
SOURCE_TOPIC = global_config["source_topic"]
PROVENANCE_TOPIC = global_config["provenance_topic"]
REQUESTS_TOPIC = global_config["requests_topic"]
REPLIES_TOPIC = global_config["replies_topic"]

TOPICS = [SOURCE_TOPIC, PROVENANCE_TOPIC, REQUESTS_TOPIC, REPLIES_TOPIC]

if __name__ == "__main__":
    import os

    args, unknown_args = get_parser().parse_known_args()
    init_logging("start-stop-services")

    if args.command == "start":
        startServices()
    elif args.command == "stop":
        stopServices()
    elif args.command == "kafka-status":
        print(f"Is kafka running? {is_kafka_running()}")
    elif args.command == "delete-topics":
        if is_kafka_running():
            delete_kafka_topics()
        else:
            logging.error("-- Kafka not running, cannot delete topics.")
    elif args.command == "soft-reset":
        soft_reset()
    elif args.command == "flink-cancel-all":
        cancel_all_flink_jobs()
    elif args.command == "kafka-reset":
        delete_kafka_topics()
        # create kafka topics
        logging.info("++ Instantiating Kafka topics now")
        createKafkaTopics()
    else:
        exit(1)
