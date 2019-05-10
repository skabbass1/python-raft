import time
import multiprocessing as mp
import random
import pickle
import pathlib
import uuid

import pytest

from raft.structures.node_config import NodeConfig
from raft.structures.log_entry import LogEntry
from raft.structures.messages import (
    AppendEntries,
    RequestVote,
    RequestVoteResponse,
    ClientRequest,
    AppendEntriesResponse,
    MajorityReplicated,
    SnapshotRequest,
    Snapshot,
    LocalStateSnapshotRequestForTesting,
)
from . import common

def test_correct_request_for_vote_gets_sent_to_all_peers(event_queues1):
    dispatcher = event_queues1.dispatcher
    peers = common.peer_node_configs()
    events = [dispatcher.get_nowait() for _ in range(1, len(peers) + 1)]
    term_log_index_log_term = [(e.term, e.last_log_index, e.last_log_term) for e in events]

    assert {p.name for p in peers} == {e.destination_server for e in events}
    assert term_log_index_log_term == [(1, 0, 0)] * len(peers)

def test_election_victory_with_majority_vote(event_queues2):
    testing_queue = event_queues2.testing
    event = testing_queue.get(timeout=1)
    assert event.state['state'] == 'leader'

def test_election_restart_without_majority_vote(event_queues3):
    state_machine_queue = event_queues3.state_machine
    testing_queue = event_queues3.testing

    state_machine_queue.put(LocalStateSnapshotRequestForTesting())
    event1 = testing_queue.get(timeout=1)

    time.sleep(0.5)

    state_machine_queue.put(LocalStateSnapshotRequestForTesting())
    event2 = testing_queue.get(timeout=1)

    assert event1.state['state'] == 'candidate'
    assert event2.state['state'] == 'candidate'
    assert event2.state['term'] > event1.state['term']


def test_legitimate_leader_discovery_mid_election(outgoing_message_queue4):
    # This is a weak test. Essentially  making sure the next election
    # begins after the latest leader term
    message = outgoing_message_queue4.get(timeout=1)
    assert message.__class__ == RequestVote
    assert message.term == 701

@pytest.fixture(name='event_queues1')
def test_correct_request_for_vote_gets_sent_to_all_peers_setup():
    event_queues = common.create_event_queues()
    startup_state = None
    initial_term = 0
    election_timeout = range(150, 300)
    commit_index = None
    log=None
    key_store=None
    peer_node_state=None

    proc = mp.Process(
            target=common.start_state_machine,
            args=(
                event_queues,
                startup_state,
                initial_term,
                election_timeout,
                commit_index,
                log,
                key_store,
                peer_node_state
                )
            )
    proc.start()

    time.sleep(1)

    yield event_queues

    proc.kill()

@pytest.fixture(name='event_queues2')
def test_election_victory_with_majority_vote_setup():
    event_queues = common.create_event_queues()
    startup_state = None
    initial_term = 0
    election_timeout = range(150, 300)
    commit_index = None
    log=None
    key_store=None
    peer_node_state=None

    proc = mp.Process(
            target=common.start_state_machine,
            args=(
                event_queues,
                startup_state,
                initial_term,
                election_timeout,
                commit_index,
                log,
                key_store,
                peer_node_state
                )
            )
    proc.start()

    # wait for election to begin and
    # then grant votes
    event_queues.dispatcher.get(timeout=1)
    for peer in ('peer2', 'peer3', 'peer5'):
        event = RequestVoteResponse(
            event_id=str(uuid.uuid4()),
            parent_event_id=None,
            source_server=peer,
            destination_server=common.leader_state_machine_name(),
            vote_granted=True,
            term=1
        )
        event_queues.state_machine.put(event)

    event_queues.state_machine.put(LocalStateSnapshotRequestForTesting())

    yield event_queues

    proc.kill()

@pytest.fixture(name='event_queues3')
def test_election_restart_without_majority_vote_setup():
    event_queues = common.create_event_queues()
    startup_state = None
    initial_term = 0
    election_timeout = range(150, 300)
    commit_index = None
    log=None
    key_store=None
    peer_node_state=None

    proc = mp.Process(
            target=common.start_state_machine,
            args=(
                event_queues,
                startup_state,
                initial_term,
                election_timeout,
                commit_index,
                log,
                key_store,
                peer_node_state
                )
            )
    proc.start()

    # wait for election to begin and
    # then grant votes
    event_queues.dispatcher.get(timeout=1)
    for peer in ('peer2',):
        event = RequestVoteResponse(
            event_id=str(uuid.uuid4()),
            parent_event_id=None,
            source_server=peer,
            destination_server=common.leader_state_machine_name(),
            vote_granted=True,
            term=1
        )
        event_queues.state_machine.put(event)

    yield event_queues

    proc.kill()

@pytest.fixture(name='outgoing_message_queue4')
def legitimate_leader_discovery_mid_election_setup():
    event_queues = {
            'state_machine': mp.Queue(),
            'communicator': mp.Queue(),
            'log_writer': None,
            'snapshot_writer': None,

            }
    startup_state = None

    proc = mp.Process(
            target=start_state_machine,
            args=(
                event_queues,
                startup_state
                )
            )
    proc.start()

    # wait for election to begin and
    # then grant votes
    event_queues['communicator'].get(timeout=1)
    for i in range(2, 3):
        message = RequestVoteResponse(
                vote_granted=True,
                term=1
                )

        message = AppendEntries(
                term=700,
                leader_id=5,
                prev_log_index=None,
                prev_log_term=None,
                leader_commit=None,
                entries=[]
                )

        event_queues['state_machine'].put(message)

    yield event_queues['communicator']

    proc.kill()

