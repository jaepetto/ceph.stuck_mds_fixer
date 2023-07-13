import json
import logging

import paramiko
from kubernetes import client, config

logger = logging.getLogger(__name__)


def get_MDS_list():
    # TODO: read MDS from a config file
    logger.info("Getting MDS list")

    return [
        "icadmin006.iccluster.epfl.ch",
        "icadmin007.iccluster.epfl.ch",
        "icadmin008.iccluster.epfl.ch",
        "icadmin011.iccluster.epfl.ch",
        "icadmin012.iccluster.epfl.ch",
        "icadmin013.iccluster.epfl.ch",
        "icadmin014.iccluster.epfl.ch",
        "icadmin015.iccluster.epfl.ch",
    ]


def get_MDS_result(MDS: str, command: str) -> str:
    logger.info(f"Getting MDS result for {MDS} and {command}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(MDS, username="root")

    stdin, stdout, stderr = ssh.exec_command(command)
    result = stdout.readlines()

    ssh.close()

    return result


def get_MDS_admin_socket_path(MDS: str) -> str:
    logger.info(f"Getting MDS admin socket path for {MDS}")
    result = get_MDS_result(MDS, "ls /var/run/ceph/*mds*.asok")
    if len(result) == 0:
        return None
    else:
        # get the first result and strip the trailing new line
        result = result[0].strip()

    return result


def get_MDS_ops_in_flight(MDS: str, adminSocketPath: str):
    command = f"ceph daemon --cluster floki {adminSocketPath} dump_ops_in_flight"
    result = get_MDS_result(MDS, command)
    result = "".join(result)
    if "ERROR: (38) Function not implemented" not in result:
        result = json.loads(result)["ops"]
    else:
        result = []
    return result


def is_waiting_for_lock(op: dict):
    if (
        "lock, waiting" in op["type_data"]["flag_point"]
        and op["type_data"]["op_type"] == "client_request"
    ):
        clientId = op["type_data"]["client_info"]["client"].split(".")[1]
        return True, clientId
    else:
        return False, None


def get_client_info(MDS: str, adminSocketPath: str, clientId: str):
    logger.info(f"Getting client info for {clientId} on {MDS}")

    sessions = get_MDS_result(
        MDS, f"ceph daemon --cluster floki {adminSocketPath} session ls"
    )
    sessions = json.loads("".join(sessions))
    for session in sessions:
        if session["id"] == int(clientId):
            host = session["client_metadata"]["hostname"]
            if "mount_point" in session["client_metadata"].keys():
                path = session["client_metadata"]["mount_point"]
            else:
                path = session["client_metadata"]["root"]
            return host, path
    return None, None


def dismount_volume(host: str, path: str) -> None:
    logger.warning(f"Dismounting {path} on {host}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username="root")

    stdin, stdout, stderr = ssh.exec_command(f"umount -f -l {path}")
    logger.debug(stdout.readlines())
    logger.debug(stderr.readlines())


def get_pod_uuid(path: str) -> str:
    logger.info(f"Getting pod uuid for {path}")
    if path.startswith("/var/lib/kubelet/pods/"):
        return path.split("/")[5]
    else:
        return None


def get_pod_info(pod_uuid: str) -> dict:
    logger.info(f"Getting pod info for {pod_uuid}")
    config.load_kube_config()
    v1 = client.CoreV1Api()
    all_pods = v1.list_pod_for_all_namespaces(watch=False)
    for pod in all_pods.items:
        if pod.metadata.uid == pod_uuid:
            return pod.metadata.name, pod.metadata.namespace
    return None, None


def delete_pod(pod_name: str, pod_namespace: str) -> None:
    logger.info(f"Deleting pod {pod_name} in namespace {pod_namespace}")
    config.load_kube_config()
    v1 = client.CoreV1Api()
    v1.delete_namespaced_pod(
        pod_name, pod_namespace, body=client.V1DeleteOptions(), grace_period_seconds=0
    )


def main() -> None:
    logger.info("Starting main function")

    MDSes = get_MDS_list()
    for MDS in MDSes:
        adminSocketPath = get_MDS_admin_socket_path(MDS)
        ops_in_flight = get_MDS_ops_in_flight(MDS, adminSocketPath)
        for op in ops_in_flight:
            isWaiting, clientId = is_waiting_for_lock(op)
            if isWaiting:
                host, path = get_client_info(MDS, adminSocketPath, clientId)
                pod_uuid = get_pod_uuid(path)
                pod_name, pod_namespace = get_pod_info(pod_uuid)
                if pod_name is not None and pod_namespace is not None:
                    delete_pod(pod_name, pod_namespace)
                else:
                    logger.warning(f"Could not find pod info for {pod_uuid}")
                dismount_volume(host, path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
