
class Machine:
    
    def __init__(self):
        self.nodeId = None
        self.membership_list = None
        self.status = None
        self.logger = None
        self.REPLICATION_FACTOR = 4
        self.WRITE_QUORUM = 4
        self.BUFFER_SIZE = 1024*128