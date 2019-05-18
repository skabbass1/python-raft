class Peer:
    def __init__(self, name, address,  next_heartbeat_time, match_index, next_index):
        self.name = name
        self.address = address
        self.next_heartbeat_time = next_heartbeat_time
        self.match_index = match_index
        self.next_index = next_index

    def __repr__(self):
        return f'{self.__class__.__name__}(name={self.name}, next_index={self.next_index}, match_index={self.match_index})'


