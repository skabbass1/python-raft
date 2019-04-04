import multiprocessing as mp
import time
import queue

import pytest

from raft.cluster_network_communicator import ClusterNetworkCommunicator
from raft.cluster_network_listener import ClusterNetworkListener
from raft.structures.node_config  import NodeConfig
from raft.structures.messages import AppendEntries

def test_sends_messages_to_connected_peers(peer_listener_queues, communicator):
    received_messages = []
    for peer_queue in peer_listener_queues:
        try:
            msg = peer_queue.get_nowait()
            received_messages.append(msg)
        except queue.Empty:
            pass
    assert len(received_messages) == 3
    expected = [
        AppendEntries(
            term=1,
            leader_id=123,
            prev_log_index=23,
            prev_log_term=0,
            entries=['set x 1'],
            leader_commit=24
        ),
        AppendEntries(
            term=1,
            leader_id=123,
            prev_log_index=23,
            prev_log_term=0,
            entries=['set x 1'],
            leader_commit=24
        ),
        AppendEntries(
            term=1,
            leader_id=123,
            prev_log_index=23,
            prev_log_term=0,
            entries=['set x 1'],
            leader_commit=24
        )
    ]

    assert expected == received_messages

@pytest.fixture(name='peer_listener_queues')
def start_peer_listeners():
    procs = []
    peer_listener_queues = []
    for node_config in peer_node_configs():
        q = mp.Queue()
        p = mp.Process(target=start_listener, args=(node_config,  q))
        peer_listener_queues.append(q)
        procs.append(p)
        p.start()

    time.sleep(1)

    yield peer_listener_queues

    for p in procs:
        p.kill()


@pytest.fixture(name='communicator')
def communicator(peer_listener_queues):
    com = mp.Process(target=start_communicator)
    com.start()

    time.sleep(1)

    yield

    com.kill()

def start_communicator():
    q = mp.Queue()
    msg = AppendEntries(
        term=1,
        leader_id=123,
        prev_log_index=23,
        prev_log_term=0,
        entries=['set x 1'],
        leader_commit=24
    )
    q.put(msg)
    communicator = ClusterNetworkCommunicator(peer_node_configs() ,q)
    communicator.run()

def start_listener(node_config,  message_queue):
    listener = ClusterNetworkListener(node_config,  message_queue)
    listener.run()

def peer_node_configs():
    return [
        NodeConfig('peer1', ('localhost', 5000)),
        NodeConfig('peer2', ('localhost', 5001)),
        NodeConfig('peer3',('localhost', 5003))
    ]

