import socket
import select
import queue
import json
import struct

class RaftClusterNetworkCommunicator:
    def __init__(self, peers, message_queue):
        self._peers = peers
        self._message_queue = message_queue

    def run(self):
        while True:
            try:
                msg = self._message_queue.get_nowait()
                self._send_to_peers(msg)
            except queue.Empty:
                pass

    def _send_to_peers(self, msg):
        for p in self._peers:
            try:
                s = self._get_connected_socket(p)
                s.send(self._pack(msg))
                s.close()
            except ConnectionRefusedError:
                # TODO handle this appropriately
                # Perhaps re-queue in the incoming message queue
                pass

    def _get_connected_socket(self, address):
         s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
         s.connect(address)
         return s

    def _pack(self, msg):
        content = json.dumps(msg).encode()
        content_len = struct.pack('>I', len(content))
        return content_len + content





