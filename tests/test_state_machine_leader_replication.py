import time
import multiprocessing as mp
import uuid

import pytest

from raft.structures.node_config import NodeConfig
from raft.structures.log_entry import LogEntry
from raft.structures.messages import (
    AppendEntries,
    AppendEntriesResponse,
    ClientRequest,
    RequestVoteResponse,
    LocalStateSnapshotRequestForTesting,
)
from raft.structures.event_trigger import EventTrigger
from . import common

def test_append_entries_associated_with_client_requests_get_tracked(append_entries_client_requests):
    event_queues, event_id1, event_id2 = append_entries_client_requests
    event = event_queues.testing.get_nowait()
    append_entries_requests = event.state['append_entries_requests']
    assert append_entries_requests == {
        (event_id1, EventTrigger.CLIENT_REQUEST): {'replicated_on_peers': set(), 'log_index_to_apply': 1},
        (event_id2, EventTrigger.CLIENT_REQUEST): {'replicated_on_peers': set(), 'log_index_to_apply': 2}
    }

def test_client_response_gets_submitted_and_client_request_gets_untracked_upon_replication_success(replication_success):
   event_queues, replicated_event, unreplicated_event = replication_success
   state = event_queues.testing.get_nowait()
   response = event_queues.client_response.get_nowait()

   assert (replicated_event.event_id, EventTrigger.CLIENT_REQUEST) not in state.state['append_entries_requests']
   assert (unreplicated_event.event_id, EventTrigger.CLIENT_REQUEST) in state.state['append_entries_requests']
   assert response.parent_event_id == replicated_event.event_id

def test_replicated_servers_count_gets_updated_on_client_requests(replicated_server_counts):
   testing_queue, client_request_event_id = replicated_server_counts
   event = testing_queue.get_nowait()
   assert event.state['client_requests'][client_request_event_id]['replicated_on_peers'] == {'peer1'}


def test_log_entry_gets_applied_to_state_machine_upon_replication_success(replication_success):
   # TODO test to ensure that order  does not matter
   testing_queue, client_queue,  client_request_event_id = replication_success
   state = testing_queue.get_nowait()
   assert  state.state['key_store'] == {'x': 176}
   assert  state.state['commit_index'] == 0

def test_uncomitted_log_entries_preceeding_the_last_replicated_log_entry_get_applied_to_state_machine(preceeding_uncommitted_log_entries):
   testing_queue, client_queue,  client_request_event_id = preceeding_uncommitted_log_entries
   state = testing_queue.get_nowait()
   assert  state.state['key_store'] == {'x': 176, 'y': 12}
   assert  state.state['commit_index'] == 1

def test_match_index_and_next_index_get_updated_upon_append_entries_success(match_index_next_index):
   testing_queue = match_index_next_index
   state = testing_queue.get_nowait()
   peer1 = state.state['peer_node_state']['peer1']
   peer2 = state.state['peer_node_state']['peer2']
   peer3 = state.state['peer_node_state']['peer3']
   peer4 = state.state['peer_node_state']['peer4']
   peer5 = state.state['peer_node_state']['peer5']

   assert peer1['next_index'] == 2
   assert peer1['match_index'] == 1

   assert peer3['next_index'] == 2
   assert peer3['match_index'] == 1

   assert peer5['next_index'] == 2
   assert peer5['match_index'] == 1

   assert peer2['next_index'] == 0
   assert peer2['match_index'] == 0

   assert peer4['next_index'] == 0
   assert peer4['match_index'] == 0

def test_leader_sends_log_entries_from_next_index_upto_the_latest_log_index_upon_client_request(next_index_to_last):
    communicator_queue = next_index_to_last
    append_entries =[communicator_queue.get_nowait() for _ in range(5)]
    peer1 = list(filter(lambda x: x.destination_server == 'peer1', append_entries))[0].entries
    peer2 = list(filter(lambda x: x.destination_server == 'peer2', append_entries))[0].entries
    peer3 = list(filter(lambda x: x.destination_server == 'peer3', append_entries))[0].entries
    peer4 = list(filter(lambda x: x.destination_server == 'peer4', append_entries))[0].entries
    peer5 = list(filter(lambda x: x.destination_server == 'peer5', append_entries))[0].entries

    assert peer1 == [LogEntry(log_index=5, term=0, command='_set', data={'key': 'x', 'value': 176})]
    assert peer2 == [LogEntry(log_index=5, term=0, command='_set', data={'key': 'x', 'value': 176})]
    assert peer3 == [LogEntry(log_index=5, term=0, command='_set', data={'key': 'x', 'value': 176})]
    assert peer4 == [
        LogEntry(log_index=2, term=0, command='_set', data={'key': 'y', 'value': 14}),
        LogEntry(log_index=3, term=0, command='_set', data={'key': 'a', 'value': 15}),
        LogEntry(log_index=4, term=0, command='_set', data={'key': 'b', 'value': 18}),
        LogEntry(log_index=5, term=0, command='_set', data={'key': 'x', 'value': 176})
    ]
    assert peer5 == [
        LogEntry(log_index=4, term=0, command='_set', data={'key': 'b', 'value': 18}),
        LogEntry(log_index=5, term=0, command='_set', data={'key': 'x', 'value': 176})
    ]

def test_leader_resends_unsuccessful_append_entries_to_followers(unsuccessful_append_entries):
    communicator_queue, testing_queue = unsuccessful_append_entries
    append_entries =[communicator_queue.get_nowait() for _ in range(6)]

    peer1 = list(filter(lambda x: x.destination_server == 'peer1', append_entries))
    first_attempt = peer1[0]
    second_attempt = peer1[1]
    assert first_attempt.prev_log_index == 4
    assert first_attempt.prev_log_term == 0
    assert first_attempt.entries == [LogEntry(
        log_index=5,
        term=0,
        command='_set',
        data={'key': 'x', 'value': 176}
    )]

    assert second_attempt.prev_log_index == 3
    assert second_attempt.prev_log_term == 0
    assert second_attempt.entries == [
        LogEntry(
            log_index=4,
            term=0,
            command='_set',
            data={'key': 'b', 'value': 18}
        ),
        LogEntry(
            log_index=5,
            term=0,
            command='_set',
            data={'key': 'x', 'value': 176}
        )
    ]

@pytest.fixture(name='append_entries_client_requests')
def test_append_entries_associated_with_client_requests_get_tracked_setup():
    event_queues = common.create_event_queues()
    startup_state = 'leader'
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

    event_id1 = str(uuid.uuid4())
    message1 = ClientRequest(
        event_id=event_id1,
        parent_event_id=None,
        event_trigger=None,
        command='_set',
        data={'key': 'x', 'value': 1}
    )
    event_id2 = str(uuid.uuid4())
    message2 = ClientRequest(
        event_id=event_id2,
        parent_event_id=None,
        event_trigger=None,
        command='_set',
        data={'key': 'y', 'value': 12}
    )
    event_queues.state_machine.put_nowait(message1)
    event_queues.state_machine.put_nowait(message2)
    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

    time.sleep(0.5)
    yield event_queues, event_id1, event_id2

    proc.kill()

@pytest.fixture(name="commit_unordered")
def test_entries_get_committed_after_unordered_replication_success_setup():
    event_queues = {
       'state_machine': mp.Queue(),
       'communicator': mp.Queue(),
       'log_writer': mp.Queue(),
       'snapshot_writer': mp.Queue()

    }
    startup_state = "leader"

    proc = mp.Process(
            target=start_state_machine,
            args=(
                event_queues,
                startup_state
            )
    )
    proc.start()

    snapshot_writer_proc = mp.Process(
            target=start_snapshot_writer,
            args=(
                'tmp',
                event_queues['state_machine'],
                event_queues['snapshot_writer']
            )
    )
    snapshot_writer_proc.start()

    message1 = ClientRequest(command='_set', data={'key': 'x', 'value': 1})
    message2 = ClientRequest(command='_set', data={'key': 'y', 'value': 12})
    message3 = ClientRequest(command='_set', data={'key': 'y', 'value': 15})

    event_queues['state_machine'].put(message1)
    event_queues['state_machine'].put(message2)
    event_queues['state_machine'].put(message3)

    message1 = MajorityReplicated(
         term=0,
         prev_log_index=None,
         prev_log_term=0,
         entries=[LogEntry(0, 0, '_set', {'key': 'x', 'value': 1})],
         leader_commit=-1
     )

    message2 = MajorityReplicated(
         term=0,
         prev_log_index=0,
         prev_log_term=0,
         entries=[LogEntry(1, 0, '_set', {'key': 'y', 'value': 12})],
         leader_commit=-1
     )

    message3 = MajorityReplicated(
         term=0,
         prev_log_index=1,
         prev_log_term=0,
         entries=[LogEntry(2, 0, '_set', {'key': 'y', 'value': 15})],
         leader_commit=-1
     )
    event_queues['state_machine'].put(message3)
    event_queues['state_machine'].put(message2)
    event_queues['state_machine'].put(message1)

    event_queues['state_machine'].put(SnapshotRequest())

    exists = wait_for_snapshot_file('tmp/snapshot_2')
    if not exists:
        proc.kill()
        snapshot_writer_proc.kill()
        pytest.fail('Snapshot Writer failed to create snapshot file within the alloted timeout')

    yield

    proc.kill()
    snapshot_writer_proc.kill()


@pytest.fixture(name='replicated_server_counts')
def test_replicated_servers_count_gets_updated_on_client_requests_setup():
    event_queues = {
       'state_machine': mp.Queue(),
       'communicator': mp.Queue(),
       'log_writer': mp.Queue(),
       'snapshot_writer': mp.Queue(),
       'testing': mp.Queue()
    }
    startup_state = "leader"

    proc = mp.Process(
            target=start_state_machine,
            args=(
                event_queues,
                startup_state
            )
    )
    proc.start()

    event_id1 = str(uuid.uuid4())
    message1 = ClientRequest(
        event_id=event_id1,
        parent_event_id=None,
        command='_set',
        data={'key': 'x', 'value': 1}
    )
    event_id2 = str(uuid.uuid4())
    message2 = ClientRequest(
        event_id=event_id2,
        parent_event_id=None,
        command='_set',
        data={'key': 'y', 'value': 12}
    )
    event_queues['state_machine'].put(message1)
    event_queues['state_machine'].put(message2)

    event_queues['state_machine'].put(
        AppendEntriesResponse(
            event_id=str(uuid.uuid4()),
            parent_event_id=event_id1,
            source_server='peer1',
            destination_server='state_machine1',
            term=0,
            success=True
        )
    )

    event_queues['state_machine'].put(LocalStateSnapshotRequestForTesting())

    time.sleep(0.5)
    yield event_queues['testing'], event_id1

    proc.kill()

@pytest.fixture(name='replication_success')
def test_client_response_get_submitted_and_client_request_gets_untracked_upon_replication_success_setup():
    event_queues = common.create_event_queues()
    startup_state = 'leader'
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

    replicated_event = ClientRequest(
        event_id=str(uuid.uuid4()),
        parent_event_id=None,
        event_trigger=None,
        command='_set',
        data={'key': 'x', 'value': 1}
    )
    unreplicated_event = ClientRequest(
        event_id=str(uuid.uuid4()),
        parent_event_id=None,
        event_trigger=None,
        command='_set',
        data={'key': 'y', 'value': 12}
    )
    event_queues.state_machine.put_nowait(replicated_event)
    event_queues.state_machine.put_nowait(unreplicated_event)

    for peer in ('peer1', 'peer3', 'peer5'):
        event_queues.state_machine.put_nowait(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                parent_event_id=replicated_event.event_id,
                event_trigger=EventTrigger.CLIENT_REQUEST,
                source_server=peer,
                destination_server='state_machine1',
                term=0,
                success=True
            )
        )

    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

    time.sleep(0.5)

    yield event_queues, replicated_event, unreplicated_event

    proc.kill()

@pytest.fixture(name='preceeding_uncommitted_log_entries')
def test_uncomitted_log_entries_preceeding_the_last_replicated_log_entry_get_applied_to_state_machine_setup():
    event_queues = {
       'state_machine': mp.Queue(),
       'communicator': mp.Queue(),
       'log_writer': mp.Queue(),
       'snapshot_writer': mp.Queue(),
       'client': mp.Queue(),
       'testing': mp.Queue()
    }
    startup_state = "leader"

    proc = mp.Process(
            target=start_state_machine,
            args=(
                event_queues,
                startup_state
            )
    )
    proc.start()

    event_id1 = str(uuid.uuid4())
    message1 = ClientRequest(
        event_id=event_id1,
        parent_event_id=None,
        command='_set',
        data={'key': 'x', 'value': 176}
    )
    event_id2 = str(uuid.uuid4())
    message2 = ClientRequest(
        event_id=event_id2,
        parent_event_id=None,
        command='_set',
        data={'key': 'y', 'value': 12}
    )
    event_queues['state_machine'].put(message1)
    event_queues['state_machine'].put(message2)

    for peer in ('peer1', 'peer3', 'peer5'):
        event_queues['state_machine'].put(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                parent_event_id=event_id2,
                source_server=peer,
                destination_server='state_machine1',
                term=0,
                success=True
            )
        )

    event_queues['state_machine'].put(LocalStateSnapshotRequestForTesting())

    time.sleep(0.5)
    yield event_queues['testing'], event_queues['client'],  event_id1

    proc.kill()

@pytest.fixture(name='match_index_next_index')
def test_match_index_and_next_index_get_updated_upon_append_entries_success_setup():
    event_queues = {
       'state_machine': mp.Queue(),
       'communicator': mp.Queue(),
       'log_writer': mp.Queue(),
       'snapshot_writer': mp.Queue(),
       'client': mp.Queue(),
       'testing': mp.Queue()
    }
    startup_state = "leader"

    proc = mp.Process(
            target=start_state_machine,
            args=(
                event_queues,
                startup_state
            )
    )
    proc.start()

    event_id1 = str(uuid.uuid4())
    message1 = ClientRequest(
        event_id=event_id1,
        parent_event_id=None,
        command='_set',
        data={'key': 'x', 'value': 176}
    )
    event_id2 = str(uuid.uuid4())
    message2 = ClientRequest(
        event_id=event_id2,
        parent_event_id=None,
        command='_set',
        data={'key': 'y', 'value': 12}
    )
    event_queues['state_machine'].put(message1)
    event_queues['state_machine'].put(message2)

    for peer in ('peer1', 'peer3', 'peer5'):
        event_queues['state_machine'].put(
            AppendEntriesResponse(
                event_id=str(uuid.uuid4()),
                parent_event_id=event_id2,
                source_server=peer,
                destination_server='state_machine1',
                term=0,
                success=True
            )
        )

    event_queues['state_machine'].put(LocalStateSnapshotRequestForTesting())

    time.sleep(0.5)
    yield event_queues['testing']

    proc.kill()

@pytest.fixture(name='next_index_to_last')
def test_leader_sends_log_entries_from_next_index_upto_the_latest_log_index_upon_client_request_setup():
    event_queues = {
       'state_machine': mp.Queue(),
       'communicator': mp.Queue(),
       'log_writer': mp.Queue(),
       'snapshot_writer': mp.Queue(),
       'client': mp.Queue(),
       'testing': mp.Queue()
    }
    startup_state = "leader"
    log = [
        LogEntry(
            log_index=1,
            term=0,
            command='_set',
            data={'key': 'x', 'value': 12}
        ),
        LogEntry(
            log_index=2,
            term=0,
            command='_set',
            data={'key': 'y', 'value': 14}
        ),
        LogEntry(
            log_index=3,
            term=0,
            command='_set',
            data={'key': 'a', 'value': 15}
        ),
        LogEntry(
            log_index=4,
            term=0,
            command='_set',
            data={'key': 'b', 'value': 18}
        ),
    ]

    key_store = {
        'x': 12,
        'y': 14,
    }

    commit_index = 2

    peer_node_state = {
        'peer1': {
            'next_index': 5,
            'match_index': 4,
            'node_state': None,
            'time_since_request': None
        },
        'peer2': {
            'next_index': 5,
            'match_index': 4,
            'node_state': None,
            'time_since_request': None
        },
        'peer3': {
            'next_index': 5,
            'match_index': 4,
            'node_state': None,
            'time_since_request': None
        },
        'peer4': {
            'next_index': 2,
            'match_index': 1,
            'node_state': None,
            'time_since_request': None
        },
        'peer5': {
            'next_index': 4,
            'match_index': 3,
            'node_state': None,
            'time_since_request': None
        },
    }


    proc = mp.Process(
            target=start_state_machine,
            args=(
                event_queues,
                startup_state,
                log,
                key_store,
                peer_node_state,
                commit_index
            )
    )
    proc.start()

    event_id1 = str(uuid.uuid4())
    message1 = ClientRequest(
        event_id=event_id1,
        parent_event_id=None,
        command='_set',
        data={'key': 'x', 'value': 176}
    )
    event_queues['state_machine'].put(message1)

    time.sleep(0.5)
    yield event_queues['communicator']

    proc.kill()

@pytest.fixture(name='unsuccessful_append_entries')
def test_leader_resends_unsuccessful_append_entries_to_followers_setup():
    event_queues = {
       'state_machine': mp.Queue(),
       'communicator': mp.Queue(),
       'log_writer': mp.Queue(),
       'snapshot_writer': mp.Queue(),
       'client': mp.Queue(),
       'testing': mp.Queue()
    }
    startup_state = "leader"
    log = [
        LogEntry(
            log_index=1,
            term=0,
            command='_set',
            data={'key': 'x', 'value': 12}
        ),
        LogEntry(
            log_index=2,
            term=0,
            command='_set',
            data={'key': 'y', 'value': 14}
        ),
        LogEntry(
            log_index=3,
            term=0,
            command='_set',
            data={'key': 'a', 'value': 15}
        ),
        LogEntry(
            log_index=4,
            term=0,
            command='_set',
            data={'key': 'b', 'value': 18}
        ),
    ]

    key_store = {
        'x': 12,
        'y': 14,
    }

    commit_index = 2

    peer_node_state = {
        'peer1': {
            'next_index': 5,
            'match_index': 4,
            'node_state': None,
            'time_since_request': None
        },
        'peer2': {
            'next_index': 5,
            'match_index': 4,
            'node_state': None,
            'time_since_request': None
        },
        'peer3': {
            'next_index': 5,
            'match_index': 4,
            'node_state': None,
            'time_since_request': None
        },
        'peer4': {
            'next_index': 2,
            'match_index': 1,
            'node_state': None,
            'time_since_request': None
        },
        'peer5': {
            'next_index': 4,
            'match_index': 3,
            'node_state': None,
            'time_since_request': None
        },
    }


    proc = mp.Process(
            target=start_state_machine,
            args=(
                event_queues,
                startup_state,
                log,
                key_store,
                peer_node_state,
                commit_index
            )
    )
    proc.start()

    event_id1 = str(uuid.uuid4())
    message1 = ClientRequest(
        event_id=event_id1,
        parent_event_id=None,
        command='_set',
        data={'key': 'x', 'value': 176}
    )
    event_queues['state_machine'].put(message1)
    event_queues['state_machine'].put(
        AppendEntriesResponse(
            event_id=str(uuid.uuid4()),
            parent_event_id=event_id1,
            source_server='peer1',
            destination_server='state_machine1',
            term=0,
            success=False
        )
    )
    event_queues['state_machine'].put(LocalStateSnapshotRequestForTesting())
    time.sleep(0.5)
    yield event_queues['communicator'], event_queues['testing']

    proc.kill()


