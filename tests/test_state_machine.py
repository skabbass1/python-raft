import time
import multiprocessing as mp
import random

import pytest

from raft.state_machine import StateMachine
from raft.structures.node_config import NodeConfig
from raft.structures.messages import (
    AppendEntries,
    RequestVote,
    RequestVoteResponse,
)

def test_election_start_after_election_timeout(outgoing_message_queue1):
    message = outgoing_message_queue1.get_nowait()
    assert message == RequestVote(
        term=1,
        candidate_id='state_machine1',
        prev_log_index=None,
        prev_log_term=None
    )

def test_election_victory_with_majority_vote(outgoing_message_queue2):
    message = outgoing_message_queue2.get(timeout=1)
    assert message.__class__ == AppendEntries
    assert message.term == 1

def test_election_restart_without_majority_vote(outgoing_message_queue3):
    message1 = outgoing_message_queue3.get(timeout=1)
    message2 = outgoing_message_queue3.get(timeout=1)

    assert message1.__class__ == RequestVote
    assert message1.term  == 2

    assert message2.__class__ == RequestVote
    assert message2.term ==  3

def test_legitimate_leader_discovery_mid_election(outgoing_message_queue4):
    # This is a weak test. Essentially  making sure the next election
    # begins after the latest leader term
    message = outgoing_message_queue4.get(timeout=1)
    assert message.__class__ == RequestVote
    assert message.term == 701

@pytest.fixture(name='outgoing_message_queue4')
def legitimate_leader_discovery_mid_election_setup():
    incoming_message_queue = mp.Queue()
    outgoing_message_queue = mp.Queue()
    proc = mp.Process(target=start_state_machine, args=(incoming_message_queue, outgoing_message_queue))
    proc.start()

    # wait for election to begin and
    # then grant votes
    outgoing_message_queue.get(timeout=1)
    for i in range(2, 3):
        message = RequestVoteResponse(
            vote_granted=True,
            term=1
        )

        incoming_message_queue.put(message)

        message = AppendEntries(
            term=700,
            leader_id=5,
            prev_log_index=None,
            prev_log_term=None,
            leader_commit=None,
            entries=[]
        )

        incoming_message_queue.put(message)

    yield outgoing_message_queue

    proc.kill()

@pytest.fixture(name='outgoing_message_queue1')
def election_start_after_timeout_setup():
    incoming_message_queue = mp.Queue()
    outgoing_message_queue = mp.Queue()
    proc = mp.Process(
            target=start_state_machine,
            args=(incoming_message_queue, outgoing_message_queue)
    )
    proc.start()

    time.sleep(1)

    yield outgoing_message_queue

    proc.kill()

@pytest.fixture(name='outgoing_message_queue3')
def election_restart_without_majority_vote_setup():
    incoming_message_queue = mp.Queue()
    outgoing_message_queue = mp.Queue()
    proc = mp.Process(target=start_state_machine, args=(incoming_message_queue, outgoing_message_queue))
    proc.start()

    # wait for election to begin and
    # then grant votes
    outgoing_message_queue.get(timeout=1)
    for i in range(2, 3):
        message = RequestVoteResponse(
            vote_granted=True,
            term=1
        )

        incoming_message_queue.put(message)

    yield outgoing_message_queue

    proc.kill()

@pytest.fixture(name='outgoing_message_queue2')
def election_victory_with_majority_vote_setup():
    incoming_message_queue = mp.Queue()
    outgoing_message_queue = mp.Queue()
    proc = mp.Process(target=start_state_machine, args=(incoming_message_queue, outgoing_message_queue))
    proc.start()

    # wait for election to begin and
    # then grant votes
    outgoing_message_queue.get(timeout=1)
    for i in range(2, 5):
        message = RequestVoteResponse(
            vote_granted=True,
            term=1
        )
        incoming_message_queue.put(message)

    yield outgoing_message_queue

    proc.kill()

def start_state_machine(incoming_message_queue, outgoing_message_queue):
    state_machine = StateMachine(
        node_config=NodeConfig(name='state_machine1', address=('localhost', 5000)),
        peer_node_configs=peer_node_configs(),
        initial_term=0,
        election_timeout=random.randint(150, 300),
        incoming_message_queue=incoming_message_queue,
        outgoing_message_queue=outgoing_message_queue
    )
    state_machine.run()

def peer_node_configs():
    return [
        NodeConfig('peer1', ('localhost', 5001)),
        NodeConfig('peer2', ('localhost', 5002)),
        NodeConfig('peer3',('localhost', 5003)),
        NodeConfig('peer4',('localhost', 5004)),
        NodeConfig('peer5',('localhost', 5005)),
    ]
