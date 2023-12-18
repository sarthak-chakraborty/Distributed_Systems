
class Machine:
    
    def __init__(self):
        self.nodeId = None
        self.membership_list = None
        self.status = None
        self.logger = None
        self.REPLICATION_FACTOR = 2
        self.WRITE_QUORUM = 2
        self.BUFFER_SIZE = 1024*256