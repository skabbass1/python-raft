import queue
import time

class RaftStateMachine:
    def __init__(
        self,
        node_id,
        term,
        election_timeout,
        incoming_message_queue,
        outgoing_message_queue
    ):

        self._term = term
        self._election_timeout = election_timeout
        self._incoming_message_queue = incoming_message_queue
        self._outgoing_message_queue = outgoing_message_queue
        self._state = 'follower'
        self._votes_received = 0
        self._start_time = None

        self._node_id = node_id
        self._last_log_index = None
        self._last_log_term = None

    def run(self):
        self._start_time = time.time()
        while True:
            try:
                msg = self._incoming_message_queue.get_nowait()
            except queue.Empty:
                elapsed_time = (time.time() - self._start_time) * 1000
                if elapsed_time > self._election_timeout:
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

