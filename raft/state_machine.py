import queue
import sys
import time

from raft.structures.messages import (
    AppendEntries,
    RequestVote,
    RequestVoteResponse,
    ClientRequest,
    MajorityReplicated,
    Snapshot,
    SnapshotRequest,
)
from raft.structures.log_entry import LogEntry

class StateMachine:
    def __init__(
        self,
        node_config,
        peer_node_configs,
        startup_state,
        initial_term,
        election_timeout,
        incoming_message_queue,
        outgoing_message_queue,
        replication_message_queue,
        log_writer_message_queue,
        snapshot_writer_message_queue
    ):

        self._node_config = node_config
        self._term = initial_term
        self._peer_node_configs = peer_node_configs
        self._election_timeout = election_timeout
        self._incoming_message_queue = incoming_message_queue
        self._outgoing_message_queue = outgoing_message_queue
        self._replication_message_queue = replication_message_queue
        self._log_writer_message_queue = log_writer_message_queue
        self._snapshot_writer_message_queue = snapshot_writer_message_queue

        self._state = startup_state or 'follower'
        self._votes_received = 0
        self._start_time = None
        self._commit_index = -1

        #TODO  boostrap log and keystore from disk
        self._log = []
        self._key_store = {}

    def run(self):
        self._start_time = time.time()
        while True:
            try:
                message = self._incoming_message_queue.get_nowait()
                if message.__class__ == RequestVoteResponse:
                    self._handle_request_for_vote_response(message)

                elif message.__class__ == AppendEntries:
                    self._handle_append_entries(message)

                elif message.__class__ == ClientRequest:
                    self._handle_client_request(message)

                elif message.__class__ == MajorityReplicated:
                    self._handle_majority_replicated(message)

                elif message.__class__ == SnapshotRequest:
                    self._handle_snapshot_request(message)

                else:
                    raise RuntimeError(f'Unandled message type {message.__class__}')
            except queue.Empty:
                elapsed_time = (time.time() - self._start_time) * 1000
                if self._state != 'leader' and  elapsed_time > self._election_timeout:
                    self._begin_election()

    def _begin_election(self):
        self._votes_received = 0
        self._term += 1
        self._state = 'candidate'
        self._votes_received +=1
        self._start_time = time.time()
        self._request_for_votes()

    def _request_for_votes(self):
        message = RequestVote(
            term=self._term,
            candidate_id=self._node_config.name,
            prev_log_index=self._log[-1].log_index if self._log else None,
            prev_log_term=self._log[-1].term if self._log else None
        )
        self._outgoing_message_queue.put_nowait(message)

    def _handle_append_entries(self, message):
        if message.term  >= self._term:
            self._state = 'follower'
            self._term = message.term


    def _handle_request_for_vote_response(self, message):
        # TODO what to do with message['term']
        if self._state == 'candidate' and message.term == self._term:
            if message.vote_granted:
                self._votes_received += 1
                if self._votes_received > len(self._peer_node_configs) - self._votes_received:
                    self._transition_to_leader_state()
                else:
                    pass
        else:
            pass

    def _handle_client_request(self, message):
       log_entry = LogEntry(
                log_index=len(self._log),
                term=self._term,
                command=message.command,
                data=message.data
        )
       self._log.append(log_entry)
       self._log_writer_message_queue.put_nowait(log_entry)

       prev_log_index = log_entry.log_index -1
       if prev_log_index < 0:
           prev_log_index = None
           prev_log_term = None
       else:
            prev_log_term = self._log[prev_log_index].term

       append_entries = AppendEntries(
            term=self._term,
            leader_id=self._node_config.name,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            leader_commit=self._commit_index,
            entries=[log_entry]
        )
       self._replication_message_queue.put_nowait(append_entries)

    def _handle_majority_replicated(self, message):
        # TODO handle different terms?
        for entry in message.entries:
            if entry.log_index > self._commit_index:
                for index in range(self._commit_index + 1, entry.log_index + 1):
                     self._commit_log_entry(self._log[index])
                     self._commit_index = entry.log_index

    def _handle_snapshot_request(self, message):
        self._snapshot_writer_message_queue.put_nowait(
            Snapshot(commit_index=self._commit_index, data=self._key_store)
        )

    def _transition_to_leader_state(self):
        self._state = 'leader'
        self._votes_recived = 0
        self._send_heartbeat()

    def _send_heartbeat(self):
        prev_log_index = len(self._log) - 1
        message = AppendEntries(
            term=self._term,
            leader_id=self._node_config.name,
            prev_log_index=prev_log_index,
            prev_log_term=-1 if prev_log_index < 0 else self._log[prev_log_index].term,
            leader_commit=self._commit_index,
            entries=[]
        )
        self._outgoing_message_queue.put_nowait(message)

    def _commit_log_entry(self, log_entry):
        self._apply_command(log_entry[2], log_entry[3])


    def _apply_command(self, command, data):
        command_func = getattr(self, command)
        command_func(data)

    def _set(self, data):
        self._key_store[data['key']] = data['value']


