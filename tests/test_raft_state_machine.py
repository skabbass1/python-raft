import time
import multiprocessing as mp
import random

import pytest

from raft.raft_state_machine import RaftStateMachine

def test_election_start_after_election_timeout(state_machine):
    """
    it initiates election with a RequestVote RPC call  after election timeout expires
    """
    item = state_machine.get_nowait()
    assert item == {
        'message_type': 'RequestVote',
        'args': {
            'term': 1,
            'candidate_id': 1,
            'last_log_index': None,
            'last_log_term': None
        }
    }



@pytest.fixture(name='state_machine')
def setup_raft_state_machine():
    incoming = mp.Queue()
    outgoing = mp.Queue()
    p = mp.Process(target=start_state_machine, args=(incoming, outgoing))
    p.start()
    time.sleep(1)

    yield outgoing

    p.kill()

def start_state_machine(incoming, outgoing):
    state_machine = RaftStateMachine(0, random.randint(150, 300), incoming, outgoing)
    state_machine.run()
