import multiprocessing as mp
import time

import pytest

from raft.cluster_network_communicator import ClusterNetworkCommunicator
from raft.cluster_network_listener import ClusterNetworkListener
from raft.structures.node_config  import NodeConfig
from raft.structures.messages import AppendEntries

def test_sends_messages_to_connected_peers(peer_queues):
    received_messages = []
    for peer_queue in peer_queues:
        try:
            msg = peer_queue.get_nowait()
            # TODO improve this by removing the starting up message
            if msg == b'Starting up':
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

@pytest.fixture(name='peer_queues')
def start_peer_listeners():
    peer_nodes = [NodeConfig('peer1', ('localhost', 5000)), NodeConfig('peer2', ('localhost', 5001)), NodeConfig('peer3',('localhost', 5003))]
    procs = []
    peer_queues = []
    for peer in peer_nodes:
        q = mp.Queue()
        p = mp.Process(target=start_listener, args=(peer.name, peer.address,  q))
        peer_queues.append(q)
        procs.append(p)
        p.start()

    communicator = mp.Process(target=start_communicator, args=(peer_nodes,))
    communicator.start()

    time.sleep(1)
    yield peer_queues

    for p in procs:
        p.kill()

    communicator.kill()

def start_listener(peer_name, address, message_queue):
    listener = ClusterNetworkListener(address, peer_name, message_queue)
    listener.run()

def start_communicator(peers):
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
    communicator = ClusterNetworkCommunicator(peers,q)
    communicator.run()
