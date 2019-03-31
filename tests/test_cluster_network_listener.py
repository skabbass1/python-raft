import multiprocessing as mp
import queue
import socket
import struct
import time

import pytest

from raft.cluster_network_listener import ClusterNetworkListener
from raft.structures.node_config import NodeConfig
from raft.structures.messages import (
    AppendEntries,
    to_json
)

def test_raft_cluster_network_listener_processes_messages(listener_message_queue):
    message = AppendEntries(
        term=1,
        leader_id=123,
        prev_log_index=23,
        prev_log_term=0,
        entries=['set x 1'],
        leader_commit=24
    )

    send_message(message)

    try:
        result  = listener_message_queue.get(timeout=5)
    except queue.Empty:
        pytest.fail('Raftlistener failed to communicate within allotted timeout of 5 seconds')

    assert result  == message


@pytest.fixture(name='listener_message_queue')
def setup_raft_listener():
    message_queue = mp.Queue()
    proc = mp.Process(target=start_listener, args=(message_queue,))
    proc.start()

    time.sleep(1)

    yield message_queue

    proc.kill()

def start_listener(message_queue):
    listener = ClusterNetworkListener(listener_node_config(), message_queue)
    listener.run()

def listener_node_config():
    return NodeConfig(name='listener1', address=('localhost', 5000))

def send_message(message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(listener_node_config().address)
    msg_bytes = to_json(message).encode()
    payload = struct.pack('>I', len(msg_bytes)) + msg_bytes
    s.send(payload)
    s.close()

