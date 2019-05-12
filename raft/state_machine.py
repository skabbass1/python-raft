import queue
import sys
import random
import time
import uuid
from collections import defaultdict

from raft.structures.messages import (
    AppendEntries,
    RequestVote,
    RequestVoteResponse,
    ClientRequest,
    ClientRequestResponse,
    AppendEntriesResponse,
    Snapshot,
    SnapshotRequest,
    LocalStateSnapshotRequestForTesting,
    LocalStateSnapshotForTesting,
)
from raft.structures.log_entry import LogEntry
from raft.structures.event_trigger import EventTrigger

class StateMachine:
    def __init__(
        self,
        node_config,
        peer_node_configs,
        startup_state,
        initial_term,
        election_timeout,
        event_queues,
        #TODO Improve the initilialization here
        commit_index=None,
        log=None,
        key_store=None,
        peer_node_state=None
    ):

        self._event_queues = event_queues

        self._node_config = node_config
        self._peer_node_configs = peer_node_configs

        self._election_timeout_range = election_timeout
        self._election_timeout = random.randint(
                self._election_timeout_range.start,
                self._election_timeout_range.stop
                )
        self._start_time = None

        self._term = initial_term
        self._state = startup_state or 'follower'
        self._votes_received = 0
        self._commit_index = commit_index or 0

        # replication related

        # TODO rename to follower
        self._peer_node_state = peer_node_state or {
                name:{
                    'next_index': 0,
                    'match_index': 0,
                    'node_state': None,
                    'time_since_request': None
               } for name, _ in peer_node_configs
        }

        # TODO rename to a more appropriate name
        self._append_entries_requests = {}


        #TODO  boostrap log and keystore from disk
        self._log = log or []
        self._key_store = key_store or {}

    def run(self):
        self._start_time = time.time()
        event_queue = self._event_queues.state_machine
        while True:
            try:
                event = event_queue.get_nowait()
                if event.__class__ == RequestVoteResponse:
                    self._handle_request_for_vote_response(event)

                elif event.__class__ == AppendEntries:
                    self._handle_append_entries(event)

                elif event.__class__ == AppendEntriesResponse:
                    self._handle_append_entries_response(event)

                elif event.__class__ == ClientRequest:
                    self._handle_client_request(event)

                elif event.__class__ == SnapshotRequest:
                    self._handle_snapshot_request(event)

                elif event.__class__ == LocalStateSnapshotRequestForTesting:
                    self._handle_local_state_snapshot_request_for_testing(event)

                else:
                    raise RuntimeError(f'Unandled event type {event.__class__}')
            except queue.Empty:
                elapsed_time = (time.time() - self._start_time) * 1000
                if self._state != 'leader' and  elapsed_time > self._election_timeout:
                    self._begin_election()

    def _begin_election(self):
        self._votes_received = 0
        self._term += 1
        self._state = 'candidate'
        self._votes_received +=1
        self._election_timeout = random.randint(
            self._election_timeout_range.start,
            self._election_timeout_range.stop
        )
        self._start_time = time.time()
        self._request_for_votes()

    def _request_for_votes(self):
        # TODO handle network failures with request for vote calls
        for peer_node in self._peer_node_configs:
            event = RequestVote(
                event_id=str(uuid.uuid4()),
                parent_event_id=None,
                event_trigger=None,
                source_server=self._node_config.name,
                destination_server=peer_node.name,
                term=self._term,
                candidate_id=self._node_config.name,
                last_log_index=len(self._log) if self._log else 0,
                last_log_term=self._log[-1].term if self._log else 0
            )
            self._event_queues.dispatcher.put_nowait(event)

    def _handle_append_entries(self, event):
        if event.term  >= self._term:
            self._state = 'follower'
            self._term = event.term


    def _handle_request_for_vote_response(self, event):
        # TODO what to do with event['term']
        if self._state == 'candidate' and event.term == self._term:
            if event.vote_granted:
                self._votes_received += 1
                if self._votes_received > len(self._peer_node_configs) - self._votes_received:
                    self._transition_to_leader_state()
                else:
                    pass
        else:
            # TODO What do do if no vote?
            pass

    def _handle_client_request(self, event):
       log_entry = LogEntry(
                log_index=len(self._log) + 1,
                term=self._term,
                command=event.command,
                data=event.data
        )
       self._log.append(log_entry)
       self._event_queues.log_writer.put_nowait(log_entry)

       prev_log_index = log_entry.log_index -1
       if prev_log_index == 0:
           prev_log_index = 0
           prev_log_term = 0
       else:
           prev_log_term = self._log[prev_log_index].term

       for node_name, state  in  self._peer_node_state.items():
           # TODO Check if node is in good state. No need
           # to keep sending new  events to a dead node. Start sending again
           # once hearbeat succeeds. Although a single failed append entries will
           # keep looping
           entries_to_send = self._log[state['next_index'] - 1:]
           append_entries = AppendEntries(
               event_id=str(uuid.uuid4()),
               parent_event_id=event.event_id,
               event_trigger=EventTrigger.CLIENT_REQUEST,
               source_server=self._node_config.name,
               destination_server=node_name,
               term=self._term,
               leader_id=self._node_config.name,
               prev_log_index=prev_log_index,
               prev_log_term=prev_log_term,
               leader_commit=self._commit_index,
               entries=entries_to_send
           )
           self._event_queues.dispatcher.put_nowait(append_entries)
           self._append_entries_requests[(event.event_id, EventTrigger.CLIENT_REQUEST)] = {
               'replicated_on_peers': set(),
               'log_index_to_apply': log_entry.log_index
           }


    def _handle_append_entries_response(self, event):
        empty = object()
        nodes = self._append_entries_requests.get((event.parent_event_id, event.event_trigger), empty)
        # TODO What about append entries reposnses not triggered byt client requestys
        if nodes is not empty:
            if event.success:
                nodes['replicated_on_peers'].add(event.source_server)
                next_index = self._peer_node_state[event.source_server]['next_index']
                match_index  = self._peer_node_state[event.source_server]['match_index']

                new_next_index = nodes['log_index_to_apply'] + 1
                if new_next_index > next_index:
                    self._peer_node_state[event.source_server]['next_index'] = new_next_index
                if nodes['log_index_to_apply'] > match_index:
                    self._peer_node_state[event.source_server]['match_index'] = nodes['log_index_to_apply']

                if len(nodes['replicated_on_peers']) > len(self._peer_node_configs) - len(nodes['replicated_on_peers']):
                    # TODO ensure you keep retrying on nodes on whom replication has not yet succeeded
                    self._apply_log_index(nodes['log_index_to_apply'])
                    self._event_queues.client_response.put_nowait(
                        ClientRequestResponse(
                            event_id=str(uuid.uuid4()),
                            parent_event_id=event.parent_event_id,
                            event_trigger=None,
                            success=True
                        )
                    )
                    del self._append_entries_requests[(event.parent_event_id, event.event_trigger)]
            else:
                self._peer_node_state[event.source_server]['next_index'] -=1
                entries_to_send = self._log[self._peer_node_state[event.source_server]['next_index'] - 1:]
                append_entries = AppendEntries(
                    event_id=str(uuid.uuid4()),
                    parent_event_id=event.parent_event_id,
                    source_server=self._node_config.name,
                    destination_server=event.source_server,
                    term=self._term,
                    leader_id=self._node_config.name,
                    prev_log_index=entries_to_send[0].log_index - 1,
                    prev_log_term=entries_to_send[0].term,
                    leader_commit=self._commit_index,
                    entries=entries_to_send
                )
                self._event_queues['communicator'].put_nowait(append_entries)

    def _apply_log_index(self, log_index):
        # TODO handle different terms?
        if log_index > self._commit_index:
            for index in range(self._commit_index + 1, log_index + 1):
                 self._commit_log_entry(self._log[index])
                 self._commit_index = log_index

    def _handle_snapshot_request(self, message):
        self._event_queues['snapshot_writer'].put_nowait(
            Snapshot(commit_index=self._commit_index, data=self._key_store)
        )

    def _handle_local_state_snapshot_request_for_testing(self, message):
        self._event_queues.testing.put_nowait( LocalStateSnapshotForTesting(state={
            'peer_node_state': self._peer_node_state,
            'state': self._state,
            'term': self._term,
            'commit_index': self._commit_index,
            'append_entries_requests': self._append_entries_requests,
            'key_store': self._key_store
           }
        ))


    def _transition_to_leader_state(self):
        self._state = 'leader'
        self._votes_recived = 0
        self._send_heartbeat()
        self._initialize_next_index()


    def _initialize_next_index(self):
        for node_name, _ in self._peer_node_configs:
            self._peer_node_state[node_name]['next_index'] = len(self._log) + 1
            self._peer_node_state[node_name]['match_index'] = 0

    def _send_heartbeat(self):
        for node_name, _ in  self._peer_node_state.items():
            append_entries = AppendEntries(
                event_id=str(uuid.uuid4()),
                parent_event_id=None,
                event_trigger=None,
                source_server=self._node_config.name,
                destination_server=node_name,
                term=self._term,
                leader_id=self._node_config.name,
                prev_log_index=self._log[-1].log_index if self._log else 0,
                prev_log_term=self._log[-1].log_term if self._log else 0,
                leader_commit=self._commit_index,
                entries=[]
            )
            self._event_queues.dispatcher.put_nowait(append_entries)

    def _commit_log_entry(self, log_entry):
        self._apply_command(log_entry[2], log_entry[3])


    def _apply_command(self, command, data):
        command_func = getattr(self, command)
        command_func(data)

    def _set(self, data):
        self._key_store[data['key']] = data['value']


