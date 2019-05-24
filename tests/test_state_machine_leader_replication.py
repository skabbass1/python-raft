import time
import multiprocessing as mp
import uuid

import pytest

from raft.structures.node_config import NodeConfig
from raft.structures.log_entry import LogEntry
from raft.structures.events import (
    AppendEntries,
    AppendEntriesResponse,
    ClientRequest,
    RequestVoteResponse,
    LocalStateSnapshotRequestForTesting,
)
from raft.structures.event_trigger import EventTrigger
from . import common

def test_peer_match_index_and_next_index_get_updated_on_replication_success(replication_success_quorum):
    event = replication_success_quorum.testing.get_nowait()
    peers = event.state['peers']

    assert peers['peer1'].match_index == 1
    assert peers['peer1'].next_index == 2
    assert peers['peer3'].match_index == 1
    assert peers['peer3'].next_index == 2
    assert peers['peer5'].match_index == 1
    assert peers['peer5'].next_index == 2
    assert peers['peer2'].match_index == 0
    assert peers['peer2'].next_index == 1
    assert peers['peer4'].match_index == 0
    assert peers['peer4'].next_index == 1

def test_peer_match_index_and_next_index_get_updated_correctly_on_unordered_replication_success_notifications(replication_success_unordered_quorum):
    event = replication_success_unordered_quorum.testing.get_nowait()
    peers = event.state['peers']

    assert peers['peer1'].match_index == 3
    assert peers['peer1'].next_index == 4
    assert peers['peer3'].match_index == 3
    assert peers['peer3'].next_index == 4
    assert peers['peer5'].match_index == 3
    assert peers['peer5'].next_index == 4
    assert peers['peer2'].match_index == 3
    assert peers['peer2'].next_index == 4
    assert peers['peer4'].match_index == 3
    assert peers['peer4'].next_index == 4

def test_peer_match_index_and_next_index_get_updated_on_replication_failure(replication_success_quorum):
    event = replication_success_quorum.testing.get_nowait()
    peers = event.state['peers']

    # failed replication
    assert peers['peer2'].match_index == 0
    assert peers['peer2'].next_index == 1
    assert peers['peer4'].match_index == 0
    assert peers['peer4'].next_index == 1

    # successful replication
    assert peers['peer1'].match_index == 1
    assert peers['peer1'].next_index == 2
    assert peers['peer3'].match_index == 1
    assert peers['peer3'].next_index == 2
    assert peers['peer5'].match_index == 1
    assert peers['peer5'].next_index == 2

def test_commit_index_advances_and_log_entries_get_committed_upon_replication_success_quorum(replication_success_unordered_quorum):
    testing_queue = replication_success_unordered_quorum.testing
    event = testing_queue.get_nowait()
    assert event.state['commit_index'] == 3
    assert event.state['key_store'] == {'x': 1, 'y': 2, 'z': 22}

def test_commit_index_and_commited_entries_do_not_change_when_replication_success_quorum_pending(replication_success_quorum_pending):
    testing_queue = replication_success_quorum_pending.testing
    event = testing_queue.get_nowait()
    assert event.state['commit_index'] == 0
    assert event.state['key_store'] == {}

def test_client_responses_get_submitted_upon_replication_success_quorum(client_request_replication_success):
    event_queues, fulfilled_request_id, unfulfilled_request_id = client_request_replication_success
    state_event=event_queues.testing.get_nowait()
    response_event=event_queues.client_response.get_nowait()
    assert len(state_event.state['pending_client_requests']) == 1
    assert state_event.state['pending_client_requests'][0].request_id == unfulfilled_request_id

    assert response_event.request_id == fulfilled_request_id

@pytest.fixture(name='replication_success_quorum_pending')
def replication_success_quorum_pending_setup():
    event_queues = common.create_event_queues()
    peers=None
    startup_state = 'leader'
    initial_term = 0
    election_timeout = range(150, 300)
    commit_index = None
    log=None
    key_store=None

    proc = mp.Process(
            target=common.start_state_machine,
            args=(
                event_queues,
                startup_state,
                peers,
                initial_term,
                election_timeout,
                commit_index,
                log,
                key_store,
                )
            )
    proc.start()

    message1 = ClientRequest(
        event_id=str(uuid.uuid4()),
        command='_set',
        data={'key': 'x', 'value': 1}
    )
    message2 = ClientRequest(
        event_id=str(uuid.uuid4()),
        command='_set',
        data={'key': 'y', 'value': 167}
    )
    event_queues.state_machine.put_nowait(message1)
    event_queues.state_machine.put_nowait(message2)

    for peer in ('peer1', 'peer3'):
        event_queues.state_machine.put_nowait(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                source_server=peer,
                destination_server='state_machine1',
                last_log_index=1,
                term=0,
                success=True
            )
        )

    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

    time.sleep(0.5)

    yield event_queues

    proc.kill()

@pytest.fixture(name='replication_success_quorum')
def replication_success_partial_setup():
    event_queues = common.create_event_queues()
    peers=None
    startup_state = 'leader'
    initial_term = 0
    election_timeout = range(150, 300)
    commit_index = None
    log=None
    key_store=None

    proc = mp.Process(
            target=common.start_state_machine,
            args=(
                event_queues,
                startup_state,
                peers,
                initial_term,
                election_timeout,
                commit_index,
                log,
                key_store,
                )
            )
    proc.start()

    message1 = ClientRequest(
        event_id=str(uuid.uuid4()),
        command='_set',
        data={'key': 'x', 'value': 1}
    )
    event_queues.state_machine.put_nowait(message1)

    for peer in ('peer1', 'peer3', 'peer5'):
        event_queues.state_machine.put_nowait(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                source_server=peer,
                destination_server='state_machine1',
                last_log_index=1,
                term=0,
                success=True
            )
        )
    for peer in ('peer2', 'peer4'):
        event_queues.state_machine.put_nowait(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                source_server=peer,
                destination_server='state_machine1',
                last_log_index=1,
                term=0,
                success=False
            )
        )

    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

    time.sleep(0.5)

    yield event_queues

    proc.kill()

@pytest.fixture(name='replication_success_quorum_pending')
def replication_success_quorum_pending():
    event_queues = common.create_event_queues()
    peers=None
    startup_state = 'leader'
    initial_term = 0
    election_timeout = range(150, 300)
    commit_index = None
    log=None
    key_store=None

    proc = mp.Process(
            target=common.start_state_machine,
            args=(
                event_queues,
                startup_state,
                peers,
                initial_term,
                election_timeout,
                commit_index,
                log,
                key_store,
                )
            )
    proc.start()

    message1 = ClientRequest(
        event_id=str(uuid.uuid4()),
        command='_set',
        data={'key': 'x', 'value': 1}
    )
    message2 = ClientRequest(
        event_id=str(uuid.uuid4()),
        command='_set',
        data={'key': 'y', 'value': 167}
    )
    event_queues.state_machine.put_nowait(message1)
    event_queues.state_machine.put_nowait(message2)

    for peer in ('peer1', 'peer3'):
        event_queues.state_machine.put_nowait(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                source_server=peer,
                destination_server='state_machine1',
                last_log_index=1,
                term=0,
                success=True
            )
        )

    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

    time.sleep(0.5)

    yield event_queues

    proc.kill()

@pytest.fixture(name='replication_success_unordered_quorum')
def replication_success_unordered_quorum_setup():
    event_queues = common.create_event_queues()
    peers=None
    startup_state = 'leader'
    initial_term = 0
    election_timeout = range(150, 300)
    commit_index = None
    log=None
    key_store=None

    proc = mp.Process(
            target=common.start_state_machine,
            args=(
                event_queues,
                startup_state,
                peers,
                initial_term,
                election_timeout,
                commit_index,
                log,
                key_store,
                )
            )
    proc.start()

    message1 = ClientRequest(
        event_id=str(uuid.uuid4()),
        command='_set',
        data={'key': 'x', 'value': 1}
    )
    message2 = ClientRequest(
        event_id=str(uuid.uuid4()),
        command='_set',
        data={'key': 'y', 'value': 2}
    )
    message3 = ClientRequest(
        event_id=str(uuid.uuid4()),
        command='_set',
        data={'key': 'z', 'value': 22}
    )
    event_queues.state_machine.put_nowait(message1)
    event_queues.state_machine.put_nowait(message2)
    event_queues.state_machine.put_nowait(message3)

    for peer in ('peer1', 'peer3', 'peer5'):
        event_queues.state_machine.put_nowait(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                source_server=peer,
                destination_server='state_machine1',
                last_log_index=1,
                term=0,
                success=True
            )
        )
    for peer in ('peer1', 'peer3', 'peer5'):
        event_queues.state_machine.put_nowait(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                source_server=peer,
                destination_server='state_machine1',
                last_log_index=3,
                term=0,
                success=True
            )
        )
    for peer in ('peer1', 'peer3', 'peer5'):
        event_queues.state_machine.put_nowait(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                source_server=peer,
                destination_server='state_machine1',
                last_log_index=2,
                term=0,
                success=True
            )
        )
    for peer in ('peer2', 'peer4'):
        event_queues.state_machine.put_nowait(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                source_server=peer,
                destination_server='state_machine1',
                last_log_index=3,
                term=0,
                success=True
            )
        )
    for peer in ('peer2', 'peer4'):
        event_queues.state_machine.put_nowait(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                source_server=peer,
                destination_server='state_machine1',
                last_log_index=1,
                term=0,
                success=True
            )
        )
    for peer in ('peer2', 'peer4'):
        event_queues.state_machine.put_nowait(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                source_server=peer,
                destination_server='state_machine1',
                last_log_index=2,
                term=0,
                success=True
            )
        )
    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

    time.sleep(0.5)

    yield event_queues

    proc.kill()

@pytest.fixture(name='client_request_replication_success')
def client_request_replication_success_setup():
    event_queues = common.create_event_queues()
    peers=None
    startup_state = 'leader'
    initial_term = 0
    election_timeout = range(150, 300)
    commit_index = None
    log=None
    key_store=None

    proc = mp.Process(
            target=common.start_state_machine,
            args=(
                event_queues,
                startup_state,
                peers,
                initial_term,
                election_timeout,
                commit_index,
                log,
                key_store,
                )
            )
    proc.start()

    fulfilled_request_id=str(uuid.uuid4())
    message1 = ClientRequest(
        event_id=fulfilled_request_id,
        command='_set',
        data={'key': 'x', 'value': 1}
    )

    unfulfilled_request_id=str(uuid.uuid4())
    message2 = ClientRequest(
        event_id=unfulfilled_request_id,
        command='_set',
        data={'key': 'y', 'value': 4}
    )
    event_queues.state_machine.put_nowait(message1)
    event_queues.state_machine.put_nowait(message2)

    for peer in ('peer1', 'peer3', 'peer5'):
        event_queues.state_machine.put_nowait(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                source_server=peer,
                destination_server='state_machine1',
                last_log_index=1,
                term=0,
                success=True
            )
        )

    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

    time.sleep(0.5)

    yield event_queues, fulfilled_request_id, unfulfilled_request_id

    proc.kill()

