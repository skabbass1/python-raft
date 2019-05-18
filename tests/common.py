import multiprocessing as  mp
import time

from raft.structures.event_queues import EventQueues
from raft.structures.peer import Peer
from raft.state_machine import StateMachine

def create_event_queues():
    return EventQueues(
            state_machine=mp.Queue(),
            dispatcher=mp.Queue(),
            listener=mp.Queue(),
            client_response=mp.Queue(),
            log_writer=mp.Queue(),
            testing=mp.Queue()
            )

def start_state_machine(
    event_queues,
    startup_state,
    peers,
    initial_term,
    election_timeout,
    commit_index,
    log,
    key_store,
):
    state_machine = StateMachine(
        node_config=leader_state_machine_name(),
        peers=peers or default_peers(),
        startup_state=startup_state,
        initial_term=initial_term,
        commit_index=commit_index,
        election_timeout=election_timeout,
        event_queues=event_queues,
        log=log,
        key_store=key_store,
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

def default_peers():
    return {
       'peer1':Peer(
            name='peer1',
            address=('localhost', 5001),
            next_heartbeat_time=time.monotonic() + 1,
            match_index=0,
            next_index=0
        ),
       'peer2': Peer(
            name='peer2',
            address=('localhost', 5002),
            next_heartbeat_time=time.monotonic() + 1,
            match_index=0,
            next_index=0
        ),
       'peer3':Peer(
            name='peer3',
            address=('localhost', 5003),
            next_heartbeat_time=time.monotonic() + 1,
            match_index=0,
            next_index=0
        ),
       'peer4':Peer(
            name='peer4',
            address=('localhost', 5004),
            next_heartbeat_time=time.monotonic() + 1,
            match_index=0,
            next_index=0
        ),
       'peer5':Peer(
            name='peer5',
            address=('localhost', 5005),
            next_heartbeat_time=time.monotonic() + 1,
            match_index=0,
            next_index=0
        )
    }

def leader_state_machine_name():
    return 'state_machine1'

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
