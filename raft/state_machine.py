import queue
import sys
import random
import time
import uuid
from collections import defaultdict

from raft.structures.events import (
    AppendEntries,
    RequestVote,
    RequestVoteResponse,
    ClientRequest,
    ClientRequestResponse,
    AppendEntriesResponse,
    PeerCommunicationNetworkError,
    LocalStateSnapshotRequestForTesting,
    LocalStateSnapshotForTesting,
)
from raft.structures.log_entry import LogEntry
from raft.structures.event_trigger import EventTrigger
from raft.structures.pending_client_request import PendingClientRequest

NEXT_HEARTBEAT_DELAY_SECONDS = 0.1
NETWORK_FAILURE_BACKOFF_DELAY_SECONDS = 3
CLIENT_REQUEST_TIMEOUT_SECONDS = 1


class StateMachine:
    def __init__(
        self,
        node_config,
        peers,
        startup_state,
        initial_term,
        election_timeout,
        event_queues,
        # TODO Improve the initilialization here
        commit_index=None,
        log=None,
        key_store=None,
        initialize_next_index=False,
    ):

        self._event_queues = event_queues

        self._name = node_config
        self._peers = peers

        self._election_timeout_range = election_timeout
        self._election_timeout = random.randint(
            self._election_timeout_range.start, self._election_timeout_range.stop
        )
        self._election_timeout_clock_start_time = None

        self._term = initial_term
        self._state = startup_state or "follower"
        self._votes_received = 0
        self._commit_index = commit_index or 0
        self._voted_for = None

        # TODO  boostrap log and keystore from disk
        self._log = log or []
        self._key_store = key_store or {}
        # TODO custom data structure for client requests
        self._pending_client_requests = []

        # # TODO cleanup
        if self._state == "leader" and initialize_next_index:
            self._initialize_next_index()

    def run(self):
        self._election_timeout_clock_start_time = time.monotonic()
        event_queue = self._event_queues.state_machine
        while True:
            self._process_next_event(event_queue)
            self._send_append_entries()
            self._advance_commit_index()
            self._send_client_responses()
            self._check_election_timeout_and_begin_election()

    def _process_next_event(self, event_queue):
        try:
            event = event_queue.get_nowait()

            if event.__class__ == RequestVote:
                self._handle_request_for_vote(event)

            elif event.__class__ == RequestVoteResponse:
                self._handle_request_for_vote_response(event)

            elif event.__class__ == AppendEntries:
                self._handle_append_entries(event)

            elif event.__class__ == AppendEntriesResponse:
                self._handle_append_entries_response(event)

            elif event.__class__ == ClientRequest:
                self._handle_client_request(event)

            elif event.__class__ == PeerCommunicationNetworkError:
                self._handle_peer_communication_network_error(event)

            elif event.__class__ == LocalStateSnapshotRequestForTesting:
                self._handle_local_state_snapshot_request_for_testing(event)

            else:
                raise RuntimeError(f"Unandled event type {event.__class__}")
        except queue.Empty:
            pass

    def _advance_commit_index(self):
        quorum = int(len(self._peers) / 2) + 1
        min_quorum_match_index = min(
            sorted([p.match_index for p in self._peers.values()], reverse=True)[:quorum]
        )
        if min_quorum_match_index > self._commit_index:
            for idx in range(self._commit_index + 1, min_quorum_match_index + 1):
                self._commit_log_entry(self._log[idx - 1])
                self._commit_index = idx

    def _send_client_responses(self):
        committed_requests = [
            r
            for r in self._pending_client_requests
            if r.request_commit_index <= self._commit_index
        ]

        if committed_requests:
            for request in committed_requests:
                self._event_queues.client_response.put_nowait(
                    ClientRequestResponse(
                        event_id=str(uuid.uuid4()),
                        request_id=request.request_id,
                        success=True,
                    )
                )

            self._pending_client_requests[:] = [
                r
                for r in self._pending_client_requests
                if r.request_commit_index > self._commit_index
            ]

        timedout_requests = [
            r
            for r in self._pending_client_requests
            if (time.monotonic() - r.request_receive_time)
            > CLIENT_REQUEST_TIMEOUT_SECONDS
        ]

        if timedout_requests:
            for request in timedout_requests:
                self._event_queues.client_response.put_nowait(
                    ClientRequestResponse(
                        event_id=str(uuid.uuid4()),
                        request_id=request.request_id,
                        success=False,
                    )
                )

                # TODO Not ideal computing this twice in this method
                self._pending_client_requests[:] = [
                    r
                    for r in self._pending_client_requests
                    if (time.monotonic() - r.request_receive_time)
                    < CLIENT_REQUEST_TIMEOUT_SECONDS
                ]

    def _check_election_timeout_and_begin_election(self):
        elapsed_time = (
            time.monotonic() - self._election_timeout_clock_start_time
        ) * 1000
        if self._state != "leader" and elapsed_time > self._election_timeout:
            self._begin_election()

    def _begin_election(self):
        self._votes_received = 0
        self._term += 1
        self._voted_for = None
        self._state = "candidate"
        self._votes_received += 1
        self._election_timeout = random.randint(
            self._election_timeout_range.start, self._election_timeout_range.stop
        )
        self._election_timeout_clock_start_time = time.monotonic()
        self._request_for_votes()

    def _request_for_votes(self):
        # TODO handle network failures with request for vote calls
        for peer in self._peers.values():
            event = RequestVote(
                event_id=str(uuid.uuid4()),
                source_server=self._name,
                destination_server=peer.name,
                term=self._term,
                candidate_id=self._name,
                last_log_index=len(self._log) if self._log else 0,
                last_log_term=self._log[-1].term if self._log else 0,
            )
            self._event_queues.dispatcher.put_nowait(event)

    def _handle_append_entries(self, event):
        self._election_timeout_clock_start_time = time.monotonic()
        if event.term >= self._term:
            self._state = "follower"
            self._term = event.term
            self._voted_for = None

    def _handle_request_for_vote(self, event):
        candidate_log_up_to_date = False
        vote_granted = False

        last_log_entry = (
            self._log[-1]
            if self._log
            else LogEntry(log_index=0, term=0, command=None, data=None)
        )

        if event.last_log_term > last_log_entry.term or (
            event.last_log_term == last_log_entry.term
            and event.last_log_index >= last_log_entry.log_index
        ):
            candidate_log_up_to_date = True

        # become follower in case peer is leader or
        # candidate
        if event.term > self._term:
            self._state = "follower"
            self._term = event.term
            self._voted_for = None

        if event.term == self._term:
            if candidate_log_up_to_date and self._voted_for == None:
                self._voted_for = event.source_server
                vote_granted = True
            else:
                vote_granted = False

        self._event_queues.dispatcher.put_nowait(
            RequestVoteResponse(
                event_id=str(uuid.uuid4()),
                source_server=self._name,
                destination_server=event.source_server,
                term=self._term,
                vote_granted=vote_granted,
            )
        )

    def _handle_request_for_vote_response(self, event):
        if self._state == "candidate":
            if event.term == self._term:
                if event.vote_granted:
                    self._votes_received += 1
                    if self._votes_received > len(self._peers) - self._votes_received:
                        self._transition_to_leader_state()
            elif event.term > self._term:
                self._state = "follower"
                self._term = event.term
                self._voted_for = None

    def _handle_client_request(self, event):
        log_entry = LogEntry(
            log_index=len(self._log) + 1,
            term=self._term,
            command=event.command,
            data=event.data,
        )
        self._log.append(log_entry)
        self._event_queues.log_writer.put_nowait(log_entry)
        self._pending_client_requests.append(
            PendingClientRequest(
                request_id=event.event_id,
                request_commit_index=log_entry.log_index,
                request_receive_time=time.monotonic(),
            )
        )

    def _handle_append_entries_response(self, event):
        peer = self._peers.get(event.source_server)
        if peer is None:
            # TODO log warning for unrecognized peer
            return

        if event.success:
            match_index = event.last_log_index
            if match_index > peer.match_index:
                peer.match_index = match_index
                peer.next_index = match_index + 1
        else:
            if peer.next_index > 1:
                peer.next_index -= 1

    def _handle_peer_communication_network_error(self, event):
        peer = self._peers.get(event.destination_server)
        if peer is None:
            # TODO log warning maybe
            return
        peer.backoff_until = time.monotonic() + NETWORK_FAILURE_BACKOFF_DELAY_SECONDS

    def _handle_local_state_snapshot_request_for_testing(self, message):
        self._event_queues.testing.put_nowait(
            LocalStateSnapshotForTesting(
                state={
                    "peers": self._peers,
                    "state": self._state,
                    "term": self._term,
                    "voted_for": self._voted_for,
                    "commit_index": self._commit_index,
                    "key_store": self._key_store,
                    "log": self._log,
                    "pending_client_requests": self._pending_client_requests,
                }
            )
        )

    def _transition_to_leader_state(self):
        self._state = "leader"
        self._votes_recived = 0
        self._send_append_entries()
        self._initialize_next_index()

    def _initialize_next_index(self):
        for peer in self._peers.values():
            peer.next_index = len(self._log) + 1
            peer.match_index = 0

    def _send_append_entries(self):
        if self._state != "leader":
            return
        for peer in self._peers.values():
            now = time.monotonic()
            # TODO Dont send if peer in bad state. Wait until backoff timeout
            if now > max(peer.next_heartbeat_time, peer.backoff_until):
                entries = self._log[peer.next_index - 1 :]
                if len(self._log) > 0:
                    prev_log_index = (
                        entries[0].log_index - 1 if entries else self._log[-1].log_index
                    )
                    prev_log_term = entries[0].term if entries else self._log[-1].term
                else:
                    prev_log_index = 0
                    prev_log_term = 0

                append_entries = AppendEntries(
                    event_id=str(uuid.uuid4()),
                    source_server=self._name,
                    destination_server=peer.name,
                    term=self._term,
                    leader_id=self._name,
                    prev_log_index=self._log[-1].log_index if self._log else 0,
                    prev_log_term=self._log[-1].term if self._log else 0,
                    leader_commit=self._commit_index,
                    entries=entries,
                )
                self._event_queues.dispatcher.put_nowait(append_entries)
                peer.next_heartbeat_time = (
                    time.monotonic() + NEXT_HEARTBEAT_DELAY_SECONDS
                )

    def _commit_log_entry(self, log_entry):
        self._apply_command(log_entry[2], log_entry[3])

    def _apply_command(self, command, data):
        command_func = getattr(self, command)
        command_func(data)

    def _set(self, data):
        self._key_store[data["key"]] = data["value"]
