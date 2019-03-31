import queue
import sys
import time

from raft.structures.messages import (
    AppendEntries,
    RequestVote,
    RequestVoteResponse,
)

class StateMachine:
    def __init__(
        self,
        node_config,
        peer_node_configs,
        initial_term,
        election_timeout,
        incoming_message_queue,
        outgoing_message_queue
    ):

        self._node_config = node_config
        self._term = initial_term
        self._peer_node_configs = peer_node_configs
        self._election_timeout = election_timeout
        self._incoming_message_queue = incoming_message_queue
        self._outgoing_message_queue = outgoing_message_queue

        self._state = 'follower'
        self._votes_received = 0
        self._start_time = None
        self._last_log_index = None
        self._last_log_term = None
        self._commit_index = None

    def run(self):
        self._start_time = time.time()
        while True:
            try:
                message = self._incoming_message_queue.get_nowait()
                if message.__class__ == RequestVoteResponse:
                    self._handle_request_for_vote_response(message)
                elif message.__class__ == AppendEntries:
                    self._handle_append_entries(message)
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
            last_log_index=self._last_log_index,
            last_log_term=self._last_log_term
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

    def _transition_to_leader_state(self):
        self._state = 'leader'
        self._votes_recived = 0
        self._send_heartbeat()

    def _send_heartbeat(self):
        message = AppendEntries(
            term=self._term,
            leader_id=self._node_config.name,
            prev_log_index=self._prev_log_index,
            prev_log_term=self._prev_log_term,
            leader_commit=self._commit_index
        )
        self._outgoing_message_queue.put_nowait(message)





