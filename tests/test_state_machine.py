import time
import multiprocessing as mp
import random
import pickle
import pathlib
import uuid

import pytest

from raft.state_machine import StateMachine
from raft.log_writer import LogWriter
from raft.snapshot_writer import SnapshotWriter
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
from raft.structures.log_entry import LogEntry

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
    assert message.state['state'] == 'leader'
    assert message.state['peer_node_state'] == {'peer1': 0, 'peer2': 0, 'peer3': 0, 'peer4': 0, 'peer5': 0}


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


def test_commands_get_logged_and_added_to(logged):
    # TODO Remove hardcoded log file name
    with open('tmp/0_1', 'rb') as f:
       contents = pickle.load(f)

    assert contents == [
       LogEntry(log_index=0, term=0, command='_set', data={'key': 'x', 'value': 1}),
       LogEntry(log_index=1, term=0, command='_set', data={'key': 'y', 'value': 2})
   ]

def test_entries_get_committed_after_replication_success(commit):
    with open('tmp/snapshot_1', 'rb') as f:
        contents = pickle.load(f)

    assert contents == Snapshot(commit_index=1, data={'x': 1, 'y': 12})

def test_entries_get_committed_after_unordered_replication_success(commit_unordered):
    with open('tmp/snapshot_2', 'rb') as f:
        contents = pickle.load(f)

    assert contents == Snapshot(commit_index=2, data={'x': 1, 'y': 15})

def test_client_requests_get_tracked_until_replication_success(tracked_client_requests):
    testing_queue, event_id1, event_id2 = tracked_client_requests
    message = testing_queue.get_nowait()
    client_requests = message.state['client_requests']
    assert client_requests == {
        event_id1: {'replicated_on_peers': set(), 'log_index_to_apply': 0},
        event_id2: {'replicated_on_peers': set(), 'log_index_to_apply': 1}
    }

def test_replicated_servers_count_gets_updated_on_client_requests(replicated_server_counts):
   testing_queue, client_request_event_id = replicated_server_counts
   event = testing_queue.get_nowait()
   assert event.state['client_requests'][client_request_event_id]['replicated_on_peers'] == {'peer1'}

def test_client_response_get_submitted_and_client_request_gets_untracked_upon_replication_success(client_response_submitted):
   testing_queue, client_queue,  client_request_event_id = client_response_submitted
   state = testing_queue.get_nowait()
   response = client_queue.get_nowait()
   assert client_request_event_id not in state.state['client_requests']
   assert response.parent_event_id == client_request_event_id

def test_log_entry_gets_applied_to_state_machine_upon_replication_success(client_response_submitted):
   testing_queue, client_queue,  client_request_event_id = client_response_submitted
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
@pytest.fixture(name='client_response_submitted')
def test_client_response_get_submitted_and_client_request_gets_untracked_upon_replication_success_setup():
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
                parent_event_id=event_id1,
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

@pytest.fixture(name='tracked_client_requests')
def test_client_requests_get_tracked_until_replication_success_setup():
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
    event_queues['state_machine'].put(LocalStateSnapshotRequestForTesting())

    time.sleep(0.5)
    yield event_queues['testing'], event_id1, event_id2

    proc.kill()


@pytest.fixture(name='commit')
def test_entries_get_committed_after_replication_success_setup():
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
                event_queues['snapshot_writer'],
            )
    )
    snapshot_writer_proc.start()

    message1 = ClientRequest(command='_set', data={'key': 'x', 'value': 1})
    message2 = ClientRequest(command='_set', data={'key': 'y', 'value': 12})
    event_queues['state_machine'].put(message1)
    event_queues['state_machine'].put(message2)

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
    event_queues['state_machine'].put(message1)
    event_queues['state_machine'].put(message2)

    event_queues['state_machine'].put(SnapshotRequest())

    exists = wait_for_snapshot_file('tmp/snapshot_1')
    if not exists:
        proc.kill()
        snapshot_writer_proc.kill()
        pytest.fail('Snapshot Writer failed to create snapshot file within the alloted timeout')

    yield

    proc.kill()
    snapshot_writer_proc.kill()

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
@pytest.fixture(name="logged")
def test_commands_get_logged():
    event_queues = {
       'state_machine': mp.Queue(),
       'communicator': mp.Queue(),
       'log_writer': mp.Queue(),
       'snapshot_writer': mp.Queue()

    }
    startup_state = "leader"

    state_machine_proc = mp.Process(
            target=start_state_machine,
            args=(
                event_queues,
                startup_state
            )
    )

    log_writer_proc = mp.Process(
        target=start_log_writer,
        args=('tmp', 2, event_queues['log_writer'])
    )

    state_machine_proc.start()
    log_writer_proc.start()

    message1 = ClientRequest(command='_set', data={'key': 'x', 'value': 1})
    message2 = ClientRequest(command='_set', data={'key': 'y', 'value': 2})
    event_queues['state_machine'].put(message1)
    event_queues['state_machine'].put(message2)

    exists = wait_for_log_file()
    if not exists:
        pytest.fail("LogWrite process failed to create logfile")

    yield

    state_machine_proc.kill()
    log_writer_proc.kill()



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

@pytest.fixture(name='outgoing_message_queue1')
def election_start_after_timeout_setup():
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

    time.sleep(1)

    yield event_queues['communicator']

    proc.kill()

@pytest.fixture(name='outgoing_message_queue3')
def election_restart_without_majority_vote_setup():
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

        event_queues['state_machine'].put(message)

    yield event_queues['communicator']

    proc.kill()

@pytest.fixture(name='outgoing_message_queue2')
def election_victory_with_majority_vote_setup():
    event_queues = {
       'state_machine': mp.Queue(),
       'communicator': mp.Queue(),
       'log_writer': None,
       'snapshot_writer': None,
       'testing': mp.Queue(),

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
    for i in range(2, 5):
        message = RequestVoteResponse(
            vote_granted=True,
            term=1
        )
        event_queues['state_machine'].put(message)

    event_queues['state_machine'].put(LocalStateSnapshotRequestForTesting())

    yield event_queues['testing']

    proc.kill()

def start_state_machine(
    event_queues,
    startup_state,
    log,
    key_store,
    peer_node_state,
    commit_index
):
    state_machine = StateMachine(
        node_config=NodeConfig(name='state_machine1', address=('localhost', 5000)),
        peer_node_configs=peer_node_configs(),
        startup_state=startup_state,
        initial_term=0,
        election_timeout=random.randint(150, 300),
        event_queues=event_queues,
        log=log,
        key_store=key_store,
        peer_node_state=peer_node_state,
        commit_index=commit_index
    )
    state_machine.run()

# TODO This method is duplicated in tests/test_logwriter.py.
# Pull out into common utils file
def start_log_writer(
    log_location,
    max_buffer_size,
    incoming_message_queue
):
    log_writer = LogWriter(
        log_location=log_location,
        max_buffer_size=max_buffer_size,
        incoming_message_queue=incoming_message_queue
    )
    log_writer.run()

def peer_node_configs():
    return [
        NodeConfig('peer1', ('localhost', 5001)),
        NodeConfig('peer2', ('localhost', 5002)),
        NodeConfig('peer3',('localhost', 5003)),
        NodeConfig('peer4',('localhost', 5004)),
        NodeConfig('peer5',('localhost', 5005)),
    ]

def wait_for_log_file():
    path = pathlib.Path('tmp/0_1')
    start = time.time()
    # TODO Wrap in re-usable decorator since
    # we will use this pattern else where as well
    while time.time() - start < 5.0:
        if path.exists():
            return True
    return False

def start_snapshot_writer(
    snapshot_location,
    state_machine_message_queue,
    incoming_message_queue
):
    snapshot_writer = SnapshotWriter(
        snapshot_location=snapshot_location,
        state_machine_message_queue=state_machine_message_queue,
        incoming_message_queue=incoming_message_queue
    )
    snapshot_writer.run()

def wait_for_snapshot_file(filepath):
    path = pathlib.Path(filepath)
    start = time.time()
    # TODO Wrap in re-usable decorator since
    # we will use this pattern else where as well
    while time.time() - start < 5.0:
        if path.exists():
            return True
    return False
