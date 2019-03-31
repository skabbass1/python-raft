import json
from raft.structures import messages

def test_converts_namedtuple_to_json():
    msg = messages.AppendEntries(
        term=1,
        leader_id=2,
        prev_log_index=3,
        prev_log_term=0,
        entries=['set x 1'],
        leader_commit=1
    )
    expected = {
        "message_type": "AppendEntries",
        "data": {
            "term": 1,
            "leader_id": 2,
            "prev_log_index": 3,
            "prev_log_term": 0,
            "entries": ["set x 1"],
            "leader_commit": 1
        }
    }

    result = (messages.to_json(msg))

    assert json.dumps(expected) == result

def test_converts_namedtuple_from_json():
    msg = json.dumps({
        "message_type": "AppendEntries",
        "data": {
            "term": 1,
            "leader_id": 2,
            "prev_log_index": 3,
            "prev_log_term": 0,
            "entries": ["set x 1"],
            "leader_commit": 1
        }
    })
    expected = messages.AppendEntries(
        term=1,
        leader_id=2,
        prev_log_index=3,
        prev_log_term=0,
        entries=['set x 1'],
        leader_commit=1
    )
    result = messages.from_json(msg)

    assert expected == result
