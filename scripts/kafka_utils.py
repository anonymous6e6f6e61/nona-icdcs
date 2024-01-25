import subprocess
import shlex
import time


def execute_quick(command, timeout=30):
    return subprocess.run(shlex.split(command), stdout=subprocess.PIPE, timeout=timeout).stdout \
        .decode('utf-8').strip()


def execute_quick_return_list(command, timeout=30):
    return [b.decode('utf-8') for b in
            subprocess.run(shlex.split(command), stdout=subprocess.PIPE, timeout=timeout).stdout \
                .splitlines()]


def find_in_string_list(search_terms: list, input: list) -> list:
    output = []
    for item in input:
        if all([term in item for term in search_terms]):
            output.append(item)
    return output


def delete_kafka_topics(kafka_bin, port, logging, ssh_prefix=""):
    topics = execute_quick_return_list(f"{ssh_prefix} {kafka_bin}/kafka-topics.sh --list --bootstrap-server {port}")
    if len(topics) > 0 and len(topics[0]) > 0:
        logging.info(f"[kafka_utils] found topics on Kafka at {port}: {topics}. Now deleting it/them...")
        for topic in topics:
            try:
                execute_quick(
                    f"{ssh_prefix} {kafka_bin}/kafka-topics.sh --delete --topic {topic} --bootstrap-server {port}")
            except:
                raise Exception(f"[kafka_utils] Failed to delete topic {topic}")


def create_kafka_topics(kafka_bin, port, topics, logging, ssh_prefix=""):
    logging.info(f"[kafka_utils] Creating topics on Kafka at {port}:")
    for topic in topics:
        logging.info(f"[kafka_utils]  - {topic}")
        try:
            execute_quick(
                f"{ssh_prefix} {kafka_bin}/kafka-topics.sh --bootstrap-server {port} --topic {topic} --create")
        except:
            raise Exception(f"[kafka_utils] failed to create topic {topic} on kafka at port {port}")


def init_kafka_topics(kafka_bin, topic_port_dict, logging):
    assert type(topic_port_dict) == dict, "topic_port_dict must be a dict of form topic:port"
    ports = set(topic_port_dict.values())
    # delete topics
    for port in ports:
        delete_kafka_topics(kafka_bin, port, logging)
    logging.info(f"[kafka_utils] Creating topics")
    # create topics
    for topic in topic_port_dict.keys():
        logging.info(f"[kafka_utils]  - {topic} at {topic_port_dict[topic]}")
        try:
            execute_quick(f"{kafka_bin}/kafka-topics.sh --bootstrap-server {topic_port_dict[topic]} "
                          f"--topic {topic} --create")
        except:
            raise Exception(f"[kafka_utils] failed to create topic {topic} on kafka at port {topic_port_dict[topic]}")


def is_zookeeper_running(ssh_prefix=""):
    ps = execute_quick_return_list(f"{ssh_prefix} ps ax")
    if len(find_in_string_list(["java", "QuorumPeerMain"], ps)) > 0:
        return True
    else:
        return False


def is_kafka_running(ssh_prefix=""):
    ps = execute_quick_return_list(f"{ssh_prefix} ps ax")
    if len(find_in_string_list(["kafka.Kafka", "java"], ps)) > 0:
        return True
    else:
        return False


def stop_start_kafka(kafka_bin, port, zoo_server_conf, kafka_server_conf, logging, ssh_prefix=""):
    if is_zookeeper_running(ssh_prefix=ssh_prefix):
        logging.info("[kafka_utils] Zookeeper already running, terminating it.")
        execute_quick(f"{ssh_prefix} {kafka_bin}/zookeeper-server-stop.sh")
        time.sleep(10)
    if is_kafka_running(ssh_prefix=ssh_prefix):
        logging.info("[kafka_utils] Kafka already running, terminating it.")
        execute_quick(f"{ssh_prefix} {kafka_bin}/kafka-server-stop.sh")
        time.sleep(10)

    # start zookeeper
    attempt = 0
    while not is_zookeeper_running(ssh_prefix=ssh_prefix):
        attempt += 1
        execute_quick(f"{ssh_prefix} {kafka_bin}/zookeeper-server-start.sh -daemon {zoo_server_conf}")
        if attempt == 10:
            logging.error("[kafka_utils] ERROR: cannot start Zookeeper")
            logging.error(f"[kafka_utils] Check zookeeper logs at {kafka_bin.replace('bin', 'logs')}")
            logging.error("[kafka_utils] Possible remedy: delete folder /tmp/zookeeper/version-2")
            raise Exception("Zookeeper start not possible")
        time.sleep(10)
    logging.info(f"[kafka_utils] Started Zookeeper at {port} using configuration {zoo_server_conf}")

    # start kafka, delete any existing topics
    attempt = 0
    while not is_kafka_running(ssh_prefix=ssh_prefix):
        attempt += 1
        execute_quick(f"{ssh_prefix} {kafka_bin}/kafka-server-start.sh -daemon {kafka_server_conf}")
        if attempt == 10:
            raise Exception("[kafka_utils] ERROR: cannot start Kafka")
        time.sleep(10)
    logging.info(f"[kafka_utils] Started Kafka at {port} using configuration {kafka_server_conf}")


def stop_kafka(kafka_bin, port, logging, ssh_prefix=""):
    # if is_kafka_running():
    logging.info("[kafka_utils] If Kafka running, deleting all topics and terminating it.")
    delete_kafka_topics(kafka_bin, port, logging, ssh_prefix=ssh_prefix)
    execute_quick(f"{ssh_prefix} {kafka_bin}/kafka-server-stop.sh")
    time.sleep(10)

    # if is_zookeeper_running():
    logging.info("[kafka_utils] If Zookeeper running, terminating it.")
    execute_quick(f"{ssh_prefix} {kafka_bin}/zookeeper-server-stop.sh")
    time.sleep(10)


def hard_stop_kafka(kafka_bin, logging, ssh_prefix=""):
    kafka_tmp = "/tmp/kafka-logs/"
    zook_tmp = "/tmp/zookeeper/version-2/"

    # if is_kafka_running():
    logging.info("[kafka_utils] If Kafka running, terminating it")
    execute_quick(f"{ssh_prefix} {kafka_bin}/kafka-server-stop.sh")
    logging.info(f"Now deleting {kafka_tmp}")
    execute_quick(f"{ssh_prefix} rm -rf {kafka_tmp}", timeout=300)
    time.sleep(5)

    # if is_zookeeper_running():
    logging.info("[kafka_utils] If Zookeeper running, terminating it.")
    execute_quick(f"{ssh_prefix} {kafka_bin}/zookeeper-server-stop.sh")
    logging.info(f"Now deleting {zook_tmp}")
    execute_quick(f"{ssh_prefix} rm -rf {zook_tmp}", timeout=300)
    time.sleep(5)
