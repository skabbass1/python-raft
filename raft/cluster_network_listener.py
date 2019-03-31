import socket
import sys
import select
import struct

from raft.structures.messages import from_json

class ClusterNetworkListener:
    def __init__(self, node_config, message_queue):
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._node_config = node_config
        self._message_queue = message_queue

        #TODO make configurable
        self._timeout_seconds = 1
        self._backlog = 5
        self._message_header_len = 4

    def run(self):
        print(f'RaftListener for node name "{self._node_config.name}" starting up', file=sys.stdout)
        self._listen()

    def _listen(self):
        self._server.setblocking(0)
        self._server.bind(self._node_config.address)
        self._server.listen(5)
        inputs = [self._server]
        while inputs:
            readable, writable, exceptional = select.select(inputs, [], inputs, self._timeout_seconds)
            for s in readable:
                if s is self._server:
                    connection, _ = s.accept()
                    inputs.append(connection)
                else:
                    content_len = self._read_content_len(s)
                    if content_len < 0:
                        inputs.remove(s)
                        s.close()
                    else:
                        content = self._read_content(s, content_len)
                        data = self._parse_content(content)
                        self._message_queue.put(data)

    def _read_content_len(self, s):
        header =  s.recv(self._message_header_len)
        if header:
            return struct.unpack('>I', header)[0]
        else:
            return -1

    def _read_content(self, s, content_len):
        content = s.recv(content_len)
        return content

    def _parse_content(self, content):
        return from_json(content.decode())

