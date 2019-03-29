import time
import multiprocessing as mp
import random

import pytest

from raft.state_machine import StateMachine

def test_election_start_after_election_timeout(state_machine2):
    item = state_machine2.get_nowait()
    assert item == {
        'message_type': 'RequestVote',
        'args': {
            'term': 1,
            'candidate_id': 1,
            'last_log_index': None,
            'last_log_term': None
        }
    }

def test_election_victory_with_majority_vote(state_machine):
    msg = state_machine.get(timeout=1)
    assert msg['message_type'] == 'AppendEntries'
    assert msg['args']['term'] == 1

def test_election_restart_without_majority_vote(state_machine3):
    msg1 = state_machine3.get(timeout=1)
    msg2 = state_machine3.get(timeout=1)

    assert msg1['message_type'] == 'RequestVote'
    assert msg1['args']['term'] == 2

    assert msg2['message_type'] == 'RequestVote'
    assert msg2['args']['term'] == 3

def test_legitimate_leader_discovery_mid_election(state_machine4):
    
    # This is a weak test. Essentially  making sure the next election
    # begins after the latest leader term
    msg = state_machine4.get(timeout=1)
    assert msg['message_type'] == 'RequestVote'
    assert msg['args']['term'] == 701

@pytest.fixture(name='state_machine4')
def state_machine4():
    incoming = mp.Queue()
    outgoing = mp.Queue()
    p = mp.Process(target=start_state_machine, args=(incoming, outgoing))
    p.start()

    # wait for election to begin and
    # then grant votes
    m =  outgoing.get(timeout=1)
    for i in range(2, 3):
        msg = {
            'message_type': 'RequestVoteResponse',
            'vote_granted': True,
            'term': 1
        }
        incoming.put(msg)

        msg = {
            'message_type': 'AppendEntries',
            'args': {
                'term': 700,
                'leader_id': 5,
                'prev_log_index': None,
                'prev_log_term': None,
                'leader_commit': None
            }
        }
        incoming.put(msg)

    yield outgoing

    p.kill()

@pytest.fixture(name='state_machine2')
def state_machine2():
    incoming = mp.Queue()
    outgoing = mp.Queue()
    p = mp.Process(target=start_state_machine, args=(incoming, outgoing))
    p.start()
    time.sleep(1)

    yield outgoing

    p.kill()

@pytest.fixture(name='state_machine3')
def state_machine3():
    incoming = mp.Queue()
    outgoing = mp.Queue()
    p = mp.Process(target=start_state_machine, args=(incoming, outgoing))
    p.start()

    # wait for election to begin and
    # then grant votes
    m =  outgoing.get(timeout=1)
    for i in range(2, 3):
        msg = {
            'message_type': 'RequestVoteResponse',
            'vote_granted': True,
            'term': 1
        }
        incoming.put(msg)

    yield outgoing

    p.kill()

@pytest.fixture(name='state_machine')
def setup_raft_state_machine():
    incoming = mp.Queue()
    outgoing = mp.Queue()
    p = mp.Process(target=start_state_machine, args=(incoming, outgoing))
    p.start()

    # wait for election to begin and
    # then grant votes
    outgoing.get(timeout=1)
    for i in range(2, 5):
        msg = {
            'message_type': 'RequestVoteResponse',
            'vote_granted': True,
            'term': 1
        }
        incoming.put(msg)

    yield outgoing

    p.kill()

def start_state_machine(incoming, outgoing):
    state_machine = StateMachine(
        1,
        [('p1,'), ('p2,'), ('p3,'), ('p4,'), ('p5,')],
        0,
        random.randint(150, 300),
        incoming, outgoing
    )
    state_machine.run()
