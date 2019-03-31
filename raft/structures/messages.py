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
