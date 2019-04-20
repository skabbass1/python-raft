import queue
import pickle

class LogWriter:
    def __init__(self, log_location, max_buffer_size, incoming_message_queue):
        self._log_location = log_location
        self._max_buffer_size = max_buffer_size
        self._incoming_message_queue = incoming_message_queue
        self._log_buffer = []

    def run(self):
        while True:
            try:
                message = self._incoming_message_queue.get_nowait()
                self._log_buffer.append(message)
                if len(self._log_buffer) >= self._max_buffer_size:
                    # TODO flush buffer on a timer as well
                    # since we want to minimize the probabaility
                    # of the server crashing without persisting log
                    self._flush_log_buffer_to_disk()
                    self._log_buffer.clear()
            except queue.Empty:
                pass

    def _flush_log_buffer_to_disk(self):
       index_start = self._log_buffer[0].log_index
       index_end = self._log_buffer[-1].log_index

       with open(f'{self._log_location}/{index_start}_{index_end}', 'wb') as f:
           pickle.dump(self._log_buffer, f)



