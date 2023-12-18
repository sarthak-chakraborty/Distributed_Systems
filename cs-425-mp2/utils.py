import datetime
from collections import defaultdict

class Message:
    def __init__(self, msg_type, host, membership_list, counter):
        self.type = msg_type
        self.host = host
        self.membership_list = membership_list
        self.counter = counter

        
class MembershipInfo:
    def __init__(self, host, membership_counter, clock_time, suspicion_flag=0, suspicion_start_time=None):
        self.host = host
        self.membership_counter = membership_counter
        self.heartbeat_increase_time = clock_time
        self.suspicion = {"flag": suspicion_flag, "start_time": suspicion_start_time}


class MembershipList:
    def __init__(self):
        self.active_nodes = {}
        self.cleanup_status_dict = {} # 0: not cleaning up, 1: host in cleanup state 


    def add_member(self, host, membership_counter, suspicion):
        clock_time = datetime.datetime.now()
        mem_info = MembershipInfo(host, membership_counter, clock_time, suspicion["flag"], suspicion["start_time"])
        self.active_nodes[host] = mem_info
        self.cleanup_status_dict[host] = {"flag": 0, "cleanup_start_time":None}


    def update_member(self, host, membership_counter, cleanup_status, logger):
        # In a any node says the host is failed, then it will be failed no matter what
        if cleanup_status is not None and cleanup_status["flag"] == 1:
            if self.cleanup_status_dict[host]["flag"] == 0:
                print(f'Node {host[0]}:{host[1]} has been failed [{datetime.datetime.now()}] \n')
                logger.debug(f'Node {host} has been failed')

                self.cleanup_status_dict[host]["flag"] = 1
                self.cleanup_status_dict[host]["cleanup_start_time"] = datetime.datetime.now()

        else:
            if self.cleanup_status_dict[host]["flag"] == 0:
                mem_info = self.active_nodes[host]
                if membership_counter > mem_info.membership_counter:
                    mem_info.membership_counter = membership_counter
                    mem_info.heartbeat_increase_time = datetime.datetime.now()

                    self.active_nodes[host] = mem_info


    def update_member_with_suspicion(self, host, membership_counter, suspicion, cleanup_status, logger):
        if cleanup_status is not None and cleanup_status["flag"] == 1:
            if self.cleanup_status_dict[host]["flag"] == 0:
                print(f'Node {host[0]}:{host[1]} has been failed [{datetime.datetime.now()}] \n')
                logger.debug(f'Node {host} has been failed')

                self.cleanup_status_dict[host]["flag"] = 1
                self.cleanup_status_dict[host]["cleanup_start_time"] = datetime.datetime.now()

        else:
            if self.cleanup_status_dict[host]["flag"] == 0:
                mem_info = self.active_nodes[host]

                if suspicion["flag"] == 1:
                    if membership_counter >= mem_info.membership_counter:
                        mem_info.membership_counter = membership_counter
                        if mem_info.suspicion["flag"] == 0:
                            print(f'Node {host[1]} has been suspected [{datetime.datetime.now()}] \n')
                            mem_info.suspicion["flag"] = suspicion["flag"]
                            mem_info.suspicion["start_time"] = datetime.datetime.now()
                    
                        self.active_nodes[host] = mem_info

                else:
                    if membership_counter > mem_info.membership_counter:
                        mem_info.membership_counter = membership_counter
                        mem_info.heartbeat_increase_time = datetime.datetime.now() # update heartbeat increase time only when it is ping and not suspicion message

                        if mem_info.suspicion["flag"] == 1:
                            mem_info.suspicion["flag"] = suspicion["flag"]
                            mem_info.suspicion["start_time"] = None
                        
                        self.active_nodes[host] = mem_info
                        

    def delete_member(self, host):
        if host in self.active_nodes:
            self.active_nodes.pop(host)
            self.cleanup_status_dict.pop(host)



