import json
import sys
from collections import namedtuple

AppendEntries = namedtuple(
    'AppendEntries',
    ['term', 'leader_id', 'prev_log_index', 'prev_log_term', 'entries', 'leader_commit']
)

RequestVote = namedtuple(
    'RequestVote',
    ['term', 'candidate_id', 'prev_log_index', 'prev_log_term']
)

RequestVoteResponse = namedtuple(
    'RequestVoteResponse',
    ['term', 'vote_granted']
)

ClientRequest = namedtuple(
        'ClientRequest',
        ['command', 'data']
)

MajorityReplicated = namedtuple(
         'MajorityReplicated',
         ['term', 'prev_log_index', 'prev_log_term', 'entries', 'leader_commit']
)

SnapshotRequest = namedtuple(
    'SnapshotRequest',
    []
)

Snapshot = namedtuple(
    'Snapshot',
    ['commit_index', 'data']
)

InitializeNextIndex = namedtuple(
    'InitializeNextIndex',
    ['last_log_index']
)

LocalStateSnapshotRequestForTesting = namedtuple(
    'LocalStateSnapshotRequestForTesting',
    []
)

LocalStateSnapshotForTesting = namedtuple(
    'LocalStateSnapshotForTesting',
    ['state']
)

def to_json(o):
    return json.dumps({
        'message_type': o.__class__.__name__,
        'data': o._asdict()
    })

def from_json(o):
    d = json.loads(o)
    this_module = sys.modules[__name__]
    klass = getattr(this_module, d['message_type'])
    return klass(**d['data'])



