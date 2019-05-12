import multiprocessing as  mp

from raft.structures.event_queues import EventQueues
from raft.structures.node_config import NodeConfig
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
    initial_term,
    election_timeout,
    commit_index,
    log,
    key_store,
    peer_node_state
):
    state_machine = StateMachine(
        node_config=NodeConfig(name=leader_state_machine_name(), address=('localhost', 5000)),
        peer_node_configs=peer_node_configs(),
        startup_state=startup_state,
        initial_term=initial_term,
        commit_index=commit_index,
        election_timeout=election_timeout,
        event_queues=event_queues,
        log=log,
        key_store=key_store,
        peer_node_state=peer_node_state
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
