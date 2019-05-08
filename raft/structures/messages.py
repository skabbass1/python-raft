import json
import sys
from collections import namedtuple

BaseEvent = namedtuple('BaseEvent', ('event_id', 'parent_event_id'))

EventRouting = namedtuple('EventRouting', ('source_server', 'destination_server'))

ClientRequest = namedtuple(
    'ClientRequest',
     BaseEvent._fields  + ('command', 'data')
)

ClientRequestResponse = namedtuple(
    'ClientRequestResponse',
     BaseEvent._fields  + ('success',)
)

AppendEntries = namedtuple(
    'AppendEntries',
    BaseEvent._fields + EventRouting._fields + (
        'term',
        'leader_id',
        'prev_log_index',
        'prev_log_term',
        'entries',
        'leader_commit',
   )
)

AppendEntriesResponse = namedtuple(
    'AppendEntriesResponse',
    BaseEvent._fields + EventRouting._fields + (
        'term',
        'success',
    )
)

RequestVote = namedtuple(
    'RequestVote',
    BaseEvent._fields + EventRouting._fields + (
        ('term', 'candidate_id', 'last_log_index', 'last_log_term')
    )
)

RequestVoteResponse = namedtuple(
    'RequestVoteResponse',
    ['term', 'vote_granted']
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



