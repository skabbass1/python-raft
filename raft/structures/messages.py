import json
import sys
from collections import namedtuple

AppendEntries = namedtuple(
    'AppendEntries',
    ['term', 'leader_id', 'prev_log_index', 'prev_log_term', 'entries', 'leader_commit']
)

RequestVote = namedtuple(
    'RequestVote',
    ['term', 'candidate_id', 'last_log_index', 'last_log_term']
)

RequestVoteResponse = namedtuple(
    'RequestVoteResponse',
    ['term', 'vote_granted']
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



