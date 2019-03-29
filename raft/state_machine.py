import queue
import sys
import time

class StateMachine:
    def __init__(
        self,
        node_id,
        peers,
        initial_term,
        election_timeout,
        incoming_message_queue,
        outgoing_message_queue
    ):

        self._node_id = node_id
        self._term = initial_term
        self._peers = peers
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
                msg = self._incoming_message_queue.get_nowait()
                if msg['message_type'] == 'RequestVoteResponse':
                    self._handle_request_for_vote_response(msg)
                elif msg['message_type'] == 'AppendEntries':
                    self._handle_append_entries(msg)
                else:
                    raise RuntimeError(f'Unandled message type {msg["message_type"]}')
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
        msg = {
            'message_type': 'RequestVote',
            'args': {
                'term': self._term,
                'candidate_id': self._node_id,
                'last_log_index': self._last_log_index,
                'last_log_term': self._last_log_term
            }
        }
        self._outgoing_message_queue.put_nowait(msg)

    def _handle_append_entries(self, message):
        if message['args']['term'] >= self._term:
            self._state = 'follower'
            self._term = message['args']['term']


    def _handle_request_for_vote_response(self, message):
        # TODO what to do with message['term']
        if self._state == 'candidate' and message['term'] == self._term:
            if message['vote_granted']:
                self._votes_received += 1
                if self._votes_received > len(self._peers) - self._votes_received:
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
        msg = {
            'message_type': 'AppendEntries',
            'args': {
                'term': self._term,
                'leader_id': self._node_id,
                'prev_log_index': self._last_log_index,
                'prev_log_term': self._last_log_term,
                'leader_commit': self._commit_index
            }
        }
        self._outgoing_message_queue.put_nowait(msg)




