import time
import multiprocessing as mp
import uuid

import pytest

from raft.structures.node_config import NodeConfig
from raft.structures.log_entry import LogEntry
from raft.structures.memory_log import MemoryLog
from raft.structures.events import (
    AppendEntries,
    RequestVote,
    RequestVoteResponse,
    LocalStateSnapshotRequestForTesting,
)
from . import common


def test_peer_denies_vote_if_candidate_term_less_than_peer_term(lagging_term):
    event_queues = lagging_term
    response = event_queues.dispatcher.get_nowait()
    assert response.vote_granted == False
    assert response.term == 3


def test_peer_denies_vote_if_candidate_log_not_up_to_date(lagging_log):
    event_queues = lagging_log
    response = event_queues.dispatcher.get_nowait()
    assert response.vote_granted == False
    assert response.term == 4


def test_peer_grants_vote(grant_vote):
    event_queues = grant_vote
    response = event_queues.dispatcher.get_nowait()

    assert response.vote_granted == True
    assert response.term == 4

    state = event_queues.testing.get_nowait()
    assert state.state["voted_for"] == "candidate_server"


def test_peer_does_not_vote_more_than_once_for_the_same_term(one_vote_per_term):
    event_queues = one_vote_per_term
    vote_granted_response = event_queues.dispatcher.get_nowait()
    vote_denied_response = event_queues.dispatcher.get_nowait()

    assert vote_granted_response.vote_granted == True
    assert vote_granted_response.destination_server == "candidate_server"
    assert vote_granted_response.term == 4

    assert vote_denied_response.vote_granted == False
    assert vote_denied_response.destination_server == "candidate_server2"
    assert vote_denied_response.term == 4


def test_peer_resets_voted_for_and_current_term_on_new_term_greater_than_current_term(new_term):
    event_queues = new_term
    state_after_first_request_for_vote = event_queues.testing.get_nowait()
    state_after_second_request_for_vote = event_queues.testing.get_nowait()

    assert state_after_first_request_for_vote.state["voted_for"] == "candidate_server"
    assert state_after_first_request_for_vote.state["term"] == 4

    assert state_after_second_request_for_vote.state["voted_for"] == None
    assert state_after_second_request_for_vote.state["term"] == 5


def test_peer_rejects_append_entries_if_new_term_less_than_current_term(append_entries_new_term):
    event_queues = append_entries_new_term
    event = event_queues.dispatcher.get_nowait()
    assert event.success == False
    assert event.term == 3
    assert event.last_log_index == 8


def test_peer_rejects_append_entries_on_incoherent_log(incoherent_log):
    event_queues = incoherent_log
    event = event_queues.dispatcher.get_nowait()
    assert event.success == False
    assert event.term == 4
    assert event.last_log_index == 5


def test_peer_rejects_append_entries_on_term_mismatch(term_mismatch):
    event_queues = term_mismatch
    event = event_queues.dispatcher.get_nowait()
    assert event.success == False
    assert event.term == 4
    assert event.last_log_index == 4


def test_peer_deletes_conflicting_log_entries_and_appends_new(conflicting_entries):
    event_queues = conflicting_entries
    event = event_queues.dispatcher.get_nowait()
    state = event_queues.testing.get_nowait()

    assert event.success == True
    assert event.term == 4
    assert event.last_log_index == 5

    assert state.state["log"] == [
        LogEntry(log_index=1, term=2, command="_set", data={"key": "a", "value": "foo"}),
        LogEntry(log_index=2, term=2, command="_set", data={"key": "a", "value": "foo"}),
        LogEntry(log_index=3, term=2, command="_set", data={"key": "a", "value": "foo"}),
        LogEntry(log_index=4, term=2, command="_set", data={"key": "a", "value": "foo"}),
        LogEntry(log_index=5, term=4, command="_set", data={"key": "a", "value": 1}),
    ]


def test_peer_appends_new_entries(new_entries):
    event_queues = new_entries
    event = event_queues.dispatcher.get_nowait()
    state = event_queues.testing.get_nowait()

    assert event.success == True
    assert event.term == 3
    assert event.last_log_index == 8

    assert state.state["log"] == [
        LogEntry(log_index=1, term=2, command="_set", data={"key": "a", "value": 11}),
        LogEntry(log_index=2, term=2, command="_set", data={"key": "a", "value": 11}),
        LogEntry(log_index=3, term=2, command="_set", data={"key": "a", "value": 11}),
        LogEntry(log_index=4, term=2, command="_set", data={"key": "a", "value": 11}),
        LogEntry(log_index=5, term=2, command="_set", data={"key": "a", "value": 11}),
        LogEntry(log_index=6, term=2, command="_set", data={"key": "a", "value": 11}),
        LogEntry(log_index=7, term=2, command="_set", data={"key": "a", "value": 11}),
        LogEntry(log_index=8, term=2, command="_set", data={"key": "a", "value": 11}),
    ]


def test_peer_commits_entries_and_advance_commit_index(commit_and_advance):
    event_queues = commit_and_advance
    event = event_queues.testing.get_nowait()
    assert event.state["commit_index"] == 8
    assert event.state["key_store"] == {"a": 1, "b": 2}


@pytest.fixture(name="lagging_term")
def test_peer_denies_vote_if_candidate_term_less_than_peer_term_setup():
    event_queues = common.create_event_queues()
    peers = None
    startup_state = "follower"
    initial_term = 3
    election_timeout = range(1000, 3000)
    commit_index = 0

    log = MemoryLog()
    log.append(LogEntry(log_index=1, term=3, command=None, data=None))
    log.append(LogEntry(log_index=2, term=3, command=None, data=None))

    key_store = None
    initialize_next_index = False

    event_queues.state_machine.put_nowait(
        RequestVote(
            event_id=str(uuid.uuid4()),
            source_server="candidate_server",
            destination_server="peer_server",
            term=2,
            candidate_id="candidate_server",
            last_log_index=1,
            last_log_term=1,
        )
    )

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
            initialize_next_index,
            "peer_server",
        ),
    )
    proc.start()

    time.sleep(0.1)

    yield event_queues

    proc.kill()


@pytest.fixture(name="lagging_log")
def test_peer_denies_vote_if_candidate_log_not_up_to_date_setup():
    event_queues = common.create_event_queues()
    peers = None
    startup_state = "follower"
    initial_term = 3
    election_timeout = range(1000, 3000)
    commit_index = 0

    log = MemoryLog()
    log.append(LogEntry(log_index=1, term=3, command=None, data=None))
    log.append(LogEntry(log_index=2, term=3, command=None, data=None))
    log.append(LogEntry(log_index=3, term=3, command=None, data=None))
    log.append(LogEntry(log_index=4, term=3, command=None, data=None))

    key_store = None
    initialize_next_index = False

    event_queues.state_machine.put_nowait(
        RequestVote(
            event_id=str(uuid.uuid4()),
            source_server="candidate_server",
            destination_server="peer_server",
            term=4,
            candidate_id="candidate_server",
            last_log_index=2,
            last_log_term=3,
        )
    )

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
            initialize_next_index,
            "peer_server",
        ),
    )
    proc.start()

    time.sleep(0.1)

    yield event_queues

    proc.kill()


@pytest.fixture(name="grant_vote")
def test_peer_grants_vote_setup():
    event_queues = common.create_event_queues()
    peers = None
    startup_state = "follower"
    initial_term = 3
    election_timeout = range(1000, 3000)
    commit_index = 0

    log = MemoryLog()
    log.append(LogEntry(log_index=1, term=3, command=None, data=None))
    log.append(LogEntry(log_index=2, term=3, command=None, data=None))

    key_store = None
    initialize_next_index = False

    event_queues.state_machine.put_nowait(
        RequestVote(
            event_id=str(uuid.uuid4()),
            source_server="candidate_server",
            destination_server="peer_server",
            term=4,
            candidate_id="candidate_server",
            last_log_index=6,
            last_log_term=3,
        )
    )

    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

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
            initialize_next_index,
            "peer_server",
        ),
    )
    proc.start()

    time.sleep(0.1)

    yield event_queues

    proc.kill()


@pytest.fixture(name="one_vote_per_term")
def test_peer_does_not_vote_more_than_once_for_the_same_term_setup():
    event_queues = common.create_event_queues()
    peers = None
    startup_state = "follower"
    initial_term = 3
    election_timeout = range(1000, 3000)
    commit_index = 0

    log = MemoryLog()
    log.append(LogEntry(log_index=1, term=3, command=None, data=None))
    log.append(LogEntry(log_index=2, term=3, command=None, data=None))

    key_store = None
    initialize_next_index = False

    event_queues.state_machine.put_nowait(
        RequestVote(
            event_id=str(uuid.uuid4()),
            source_server="candidate_server",
            destination_server="peer_server",
            term=4,
            candidate_id="candidate_server",
            last_log_index=6,
            last_log_term=3,
        )
    )

    event_queues.state_machine.put_nowait(
        RequestVote(
            event_id=str(uuid.uuid4()),
            source_server="candidate_server2",
            destination_server="peer_server",
            term=4,
            candidate_id="candidate_server",
            last_log_index=6,
            last_log_term=3,
        )
    )

    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

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
            initialize_next_index,
            "peer_server",
        ),
    )
    proc.start()

    time.sleep(0.1)

    yield event_queues

    proc.kill()


@pytest.fixture(name="new_term")
def test_peer_resets_voted_for_and_current_term_for_on_new_term_greater_than_current_term_setup():
    event_queues = common.create_event_queues()
    peers = None
    startup_state = "follower"
    initial_term = 3
    election_timeout = range(1000, 3000)
    commit_index = 0

    log = MemoryLog()
    log.append(LogEntry(log_index=1, term=3, command=None, data=None))
    log.append(LogEntry(log_index=2, term=3, command=None, data=None))
    log.append(LogEntry(log_index=3, term=3, command=None, data=None))
    log.append(LogEntry(log_index=4, term=3, command=None, data=None))

    key_store = None
    initialize_next_index = False

    event_queues.state_machine.put_nowait(
        RequestVote(
            event_id=str(uuid.uuid4()),
            source_server="candidate_server",
            destination_server="peer_server",
            term=4,
            candidate_id="candidate_server",
            last_log_index=6,
            last_log_term=3,
        )
    )

    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

    event_queues.state_machine.put_nowait(
        RequestVote(
            event_id=str(uuid.uuid4()),
            source_server="candidate_server2",
            destination_server="peer_server",
            term=5,
            candidate_id="candidate_server",
            last_log_index=3,
            last_log_term=3,
        )
    )

    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

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
            initialize_next_index,
            "peer_server",
        ),
    )
    proc.start()

    time.sleep(0.1)

    yield event_queues

    proc.kill()


@pytest.fixture(name="append_entries_new_term")
def test_peer_rejects_append_entries_if_new_term_less_than_current_term_setup():
    event_queues = common.create_event_queues()
    peers = None
    startup_state = "follower"
    initial_term = 3
    election_timeout = range(1000, 3000)
    commit_index = 0

    log = MemoryLog()
    log.append(LogEntry(log_index=1, term=2, command=None, data=None))
    log.append(LogEntry(log_index=2, term=2, command=None, data=None))
    log.append(LogEntry(log_index=3, term=2, command=None, data=None))
    log.append(LogEntry(log_index=4, term=2, command=None, data=None))
    log.append(LogEntry(log_index=5, term=3, command=None, data=None))
    log.append(LogEntry(log_index=6, term=3, command=None, data=None))
    log.append(LogEntry(log_index=7, term=3, command=None, data=None))
    log.append(LogEntry(log_index=8, term=3, command=None, data=None))

    key_store = None
    initialize_next_index = False

    event_queues.state_machine.put_nowait(
        AppendEntries(
            event_id=str(uuid.uuid4()),
            source_server="leader_server",
            destination_server="peer_server",
            term=2,
            leader_id="leader_server",
            prev_log_index=2,
            prev_log_term=2,
            entries=[LogEntry(log_index=3, term=2, command=None, data=None)],
            leader_commit=1,
        )
    )

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
            initialize_next_index,
            "peer_server",
        ),
    )
    proc.start()

    time.sleep(0.1)

    yield event_queues

    proc.kill()


@pytest.fixture(name="incoherent_log")
def test_peer_rejects_append_entries_on_incoherent_log_setup():
    event_queues = common.create_event_queues()
    peers = None
    startup_state = "follower"
    initial_term = 3
    election_timeout = range(1000, 3000)
    commit_index = 0

    log = MemoryLog()
    log.append(LogEntry(log_index=1, term=2, command=None, data=None))
    log.append(LogEntry(log_index=2, term=2, command=None, data=None))
    log.append(LogEntry(log_index=3, term=2, command=None, data=None))
    log.append(LogEntry(log_index=4, term=2, command=None, data=None))
    log.append(LogEntry(log_index=5, term=3, command=None, data=None))

    key_store = None
    initialize_next_index = False

    event_queues.state_machine.put_nowait(
        AppendEntries(
            event_id=str(uuid.uuid4()),
            source_server="leader_server",
            destination_server="peer_server",
            term=4,
            leader_id="leader_server",
            prev_log_index=7,
            prev_log_term=3,
            entries=[LogEntry(log_index=8, term=4, command=None, data=None)],
            leader_commit=1,
        )
    )

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
            initialize_next_index,
            "peer_server",
        ),
    )
    proc.start()

    time.sleep(0.1)

    yield event_queues

    proc.kill()


@pytest.fixture(name="term_mismatch")
def test_peer_rejects_append_entries_on_term_mismatch_setup():
    event_queues = common.create_event_queues()
    peers = None
    startup_state = "follower"
    initial_term = 3
    election_timeout = range(1000, 3000)
    commit_index = 0

    log = MemoryLog()
    log.append(LogEntry(log_index=1, term=2, command=None, data=None))
    log.append(LogEntry(log_index=2, term=2, command=None, data=None))
    log.append(LogEntry(log_index=3, term=2, command=None, data=None))
    log.append(LogEntry(log_index=4, term=2, command=None, data=None))

    key_store = None
    initialize_next_index = False

    event_queues.state_machine.put_nowait(
        AppendEntries(
            event_id=str(uuid.uuid4()),
            source_server="leader_server",
            destination_server="peer_server",
            term=4,
            leader_id="leader_server",
            prev_log_index=4,
            prev_log_term=4,
            entries=[LogEntry(log_index=5, term=4, command=None, data=None)],
            leader_commit=1,
        )
    )

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
            initialize_next_index,
            "peer_server",
        ),
    )
    proc.start()

    time.sleep(0.1)

    yield event_queues

    proc.kill()


@pytest.fixture(name="conflicting_entries")
def test_peer_deletes_conflicting_log_entries_setup():
    event_queues = common.create_event_queues()
    peers = None
    startup_state = "follower"
    initial_term = 2
    election_timeout = range(1000, 3000)
    commit_index = 0

    log = MemoryLog()
    log.append(LogEntry(log_index=1, term=2, command="_set", data={"key": "a", "value": "foo"}))
    log.append(LogEntry(log_index=2, term=2, command="_set", data={"key": "a", "value": "foo"}))
    log.append(LogEntry(log_index=3, term=2, command="_set", data={"key": "a", "value": "foo"}))
    log.append(LogEntry(log_index=4, term=2, command="_set", data={"key": "a", "value": "foo"}))
    log.append(LogEntry(log_index=5, term=2, command="_set", data={"key": "a", "value": "foo"}))
    log.append(LogEntry(log_index=6, term=2, command="_set", data={"key": "a", "value": "foo"}))
    log.append(LogEntry(log_index=7, term=2, command="_set", data={"key": "a", "value": "foo"}))

    key_store = None
    initialize_next_index = False

    event_queues.state_machine.put_nowait(
        AppendEntries(
            event_id=str(uuid.uuid4()),
            source_server="leader_server",
            destination_server="peer_server",
            term=4,
            leader_id="leader_server",
            prev_log_index=4,
            prev_log_term=2,
            entries=[LogEntry(log_index=5, term=4, command="_set", data={"key": "a", "value": 1})],
            leader_commit=1,
        )
    )

    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

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
            initialize_next_index,
            "peer_server",
        ),
    )
    proc.start()

    time.sleep(0.1)

    yield event_queues

    proc.kill()


@pytest.fixture(name="new_entries")
def test_peer_appends_new_entries_setup():
    event_queues = common.create_event_queues()
    peers = None
    startup_state = "follower"
    initial_term = 2
    election_timeout = range(1000, 3000)
    commit_index = 0

    log = MemoryLog()
    log.append(LogEntry(log_index=1, term=2, command="_set", data={"key": "a", "value": 11}))
    log.append(LogEntry(log_index=2, term=2, command="_set", data={"key": "a", "value": 11}))
    log.append(LogEntry(log_index=3, term=2, command="_set", data={"key": "a", "value": 11}))
    log.append(LogEntry(log_index=4, term=2, command="_set", data={"key": "a", "value": 11}))
    log.append(LogEntry(log_index=5, term=2, command="_set", data={"key": "a", "value": 11}))
    log.append(LogEntry(log_index=6, term=2, command="_set", data={"key": "a", "value": 11}))
    log.append(LogEntry(log_index=7, term=2, command="_set", data={"key": "a", "value": 11}))

    key_store = None
    initialize_next_index = False

    event_queues.state_machine.put_nowait(
        AppendEntries(
            event_id=str(uuid.uuid4()),
            source_server="leader_server",
            destination_server="peer_server",
            term=3,
            leader_id="leader_server",
            prev_log_index=7,
            prev_log_term=2,
            entries=[LogEntry(log_index=8, term=2, command="_set", data={"key": "a", "value": 11})],
            leader_commit=1,
        )
    )

    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

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
            initialize_next_index,
            "peer_server",
        ),
    )
    proc.start()

    time.sleep(0.1)

    yield event_queues

    proc.kill()


@pytest.fixture(name="commit_and_advance")
def test_peer_commits_entries_and_advances_commit_index_setup():
    event_queues = common.create_event_queues()
    peers = None
    startup_state = "follower"
    initial_term = 2
    election_timeout = range(1000, 3000)
    commit_index = 0

    log = MemoryLog()

    log.append(LogEntry(log_index=1, term=2, command="_set", data={"key": "a", "value": 1}))
    log.append(LogEntry(log_index=2, term=2, command="_set", data={"key": "a", "value": 1}))
    log.append(LogEntry(log_index=3, term=2, command="_set", data={"key": "a", "value": 1}))
    log.append(LogEntry(log_index=4, term=2, command="_set", data={"key": "a", "value": 1}))
    log.append(LogEntry(log_index=5, term=2, command="_set", data={"key": "a", "value": 1}))
    log.append(LogEntry(log_index=6, term=2, command="_set", data={"key": "a", "value": 1}))
    log.append(LogEntry(log_index=7, term=2, command="_set", data={"key": "a", "value": 1}))

    key_store = None
    initialize_next_index = False

    event_queues.state_machine.put_nowait(
        AppendEntries(
            event_id=str(uuid.uuid4()),
            source_server="leader_server",
            destination_server="peer_server",
            term=3,
            leader_id="leader_server",
            prev_log_index=7,
            prev_log_term=2,
            entries=[LogEntry(log_index=8, term=2, command="_set", data={"key": "b", "value": 2})],
            leader_commit=8,
        )
    )

    event_queues.state_machine.put_nowait(LocalStateSnapshotRequestForTesting())

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
            initialize_next_index,
            "peer_server",
        ),
    )
    proc.start()

    time.sleep(0.1)

    yield event_queues

    proc.kill()
