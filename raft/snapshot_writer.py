import queue
import pickle

#TODO This is temporary and will change once snapshotting is 
# properly implemented per the raft paper
class SnapshotWriter:
    def __init__(
        self,
        snapshot_location,
        state_machine_message_queue,
        incoming_message_queue
    ):
        self._snapshot_location = snapshot_location
        self._incoming_message_queue = incoming_message_queue
        self._state_machine_message_queue = state_machine_message_queue

    def run(self):
        while True:
            try:
                message = self._incoming_message_queue.get_nowait()
                self._write_snapshot_to_disk(message)
            except queue.Empty:
                pass

    def _write_snapshot_to_disk(self, message):
       with open(f'{self._snapshot_location}/snapshot_{message.commit_index}', 'wb') as f:
           pickle.dump(message, f)
