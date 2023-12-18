import socket
import sys
import threading
import pickle
import random
import datetime
import time
from utils import *
from static import *
import logging
import time

# Static variables
MAX = 8192                  # Max size of message   
OFFSET = 0
BASE_PORT = 8000 + OFFSET   # Base port number
B = 3                       # Number of nodes to gossip with
T_GOSSIP = 0.7              # Time interval for gossiping
T_FAIL = 5                  # Time interval for failure detection
T_SUSPECT = 5               # Time interval for suspicion
T_CLEANUP = 15               # Time interval for cleanup
MESSAGE_DROP_RATE = 0.0     # Message drop rate


class Failure_Detector:

    def __init__(self, MACHINE_NUM, MACHINE):
        self.MACHINE_NUM = MACHINE_NUM
        self.port = BASE_PORT + MACHINE_NUM
        self.hostname = "fa23-cs425-37" + f"{MACHINE_NUM:02d}" + ".cs.illinois.edu"
        self.ip = socket.gethostbyname(self.hostname)
        self.machine = MACHINE
        
        self.ping_counter = -1
        self.membership_mutex = threading.Lock()
        self.machine.status_mutex = threading.Lock()
        self.ENABLE_SUSPICION = True
        self.SUSPICION_RECVD = False
        self.sock_fd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        INTRODUCER = NODES[0]
        machine_num = INTRODUCER[0]
        self.introducer_ip = socket.gethostbyname(INTRODUCER[1])
        self.introducer_port = BASE_PORT + machine_num


    def send_message(self, sock_fd, msg, ip, port):
        ''' Send a message to another machine '''
        try:
            sock_fd.sendto(msg, (ip, port))
        except:
            pass


    def add_membership_list(self, mssg):
        ''' Add a new member to the membership list (when join message received) '''
        host = mssg.host
        membership_counter = mssg.counter
        self.machine.membership_list.add_member(host=host,
                                        membership_counter=membership_counter, 
                                        suspicion={"flag":0, "start_time":None}
                                        )
        

        self.machine.logger.info('Join: IP {}, Port: {}, Version: {}'.format(host[0], host[1], host[2]))
        for khost,v in self.machine.membership_list.active_nodes.items():
            self.machine.logger.debug("JoinMembershipLog: {}, Host {}, Ctr {}".format(host[1], khost, v.membership_counter))


    def update_membership_list(self, mssg):
        ''' Update the membership list '''
        msg_host = mssg.host
        msg_membership_list = mssg.membership_list

        for khost, v in self.machine.membership_list.active_nodes.items():
            self.machine.logger.debug("PrevPingerLog: {}, Host {}, Ctr {}".format(msg_host[1], khost, v.membership_counter))
        self.machine.logger.debug("\n\n")

        for khost, v in msg_membership_list.active_nodes.items():
            self.machine.logger.debug("MsgMembershipList of {}, Host {}, Ctr {}".format(msg_host, khost, v.membership_counter))
        self.machine.logger.debug("\n\n")

        # For each host in the received membership list, update the membership list of the current machine
        for host in msg_membership_list.active_nodes.keys():
            if host == self.machine.nodeId:
                continue

            # If the host is not in the current machine's membership list, add it
            if host not in self.machine.membership_list.active_nodes.keys():
                if msg_membership_list.cleanup_status_dict[host]["flag"] == 1:
                    continue
                self.machine.membership_list.add_member(host, 
                                                msg_membership_list.active_nodes[host].membership_counter, 
                                                msg_membership_list.active_nodes[host].suspicion
                                                )
            # If the host is in the current machine's membership list, update it                                    
            else:
                # Add failed node to failed_nodes list if failure is detected through gossip (not by machine itself)
                if msg_membership_list.cleanup_status_dict[host] is not None and \
                    msg_membership_list.cleanup_status_dict[host]["flag"] == 1:
                    if self.machine.membership_list.cleanup_status_dict[host]["flag"] == 0:
                            self.machine.membership_list.failed_nodes.append(host)

                if self.ENABLE_SUSPICION:
                    self.machine.membership_list.update_member_with_suspicion(host, 
                                                                        msg_membership_list.active_nodes[host].membership_counter, 
                                                                        msg_membership_list.active_nodes[host].suspicion,
                                                                        msg_membership_list.cleanup_status_dict[host],
                                                                        self.machine.logger
                                                                        )
                else:            
                    self.machine.membership_list.update_member(host, 
                                                        msg_membership_list.active_nodes[host].membership_counter,
                                                        msg_membership_list.cleanup_status_dict[host],
                                                        self.machine.logger
                                                       )

        for khost, v in self.machine.membership_list.active_nodes.items():
            self.machine.logger.debug("AfterPingerLog: {}, Host {}, Ctr {}".format(msg_host[1], khost, v.membership_counter))
        self.machine.logger.debug("\n\n")


    def remove_membership_list(self, mssg):
        ''' Remove a member from the membership list '''
        host = mssg.host
        self.machine.membership_list.delete_member(host) # delete host from current machine's membership list
        self.machine.logger.info('Left: IP {}, Port: {}, Version: {}'.format(host[0], host[1], host[2]))

        print('Leave: IP {}, Port: {}, Version: {}'.format(host[0], host[1], host[2]))
        for khost,v in self.machine.membership_list.active_nodes.items():
            self.machine.logger.debug("LeaveMembershipLog: {}, Host {}, Ctr {}".format(host[1], khost, v.membership_counter))


    def remove_member(self):
        ''' Remove a failed member from the membership list '''
        while True:
            if self.SUSPICION_RECVD:
                continue

            cleanup_host = None
            # For each host in the membership list, check if it is failed. If failed, remove it
            for host, cleanup_status in self.machine.membership_list.cleanup_status_dict.items():

                if cleanup_status["flag"] == 1:
                    time_now = datetime.datetime.now()

                    if (time_now - cleanup_status["cleanup_start_time"]).total_seconds() > T_CLEANUP:
                        cleanup_host = host
                        break
        
            if cleanup_host is not None:
                with self.membership_mutex:
                    self.machine.membership_list.delete_member(cleanup_host)
                            
                print(f'Node {cleanup_host[0]}:{cleanup_host[1]} has been removed at {datetime.datetime.now()}\n')
                self.machine.logger.debug(f'Node {cleanup_host} has been removed')
            
            time.sleep(0.2)

    
    def suspect(self):
        ''' Suspect a member to fail from the membership list if no heartbeat was received for T_SUSPECT seconds '''
        while True:
            if self.SUSPICION_RECVD:
                continue

            if self.ENABLE_SUSPICION:
                active_nodes = list(self.machine.membership_list.active_nodes.keys()).copy()
                for host in active_nodes:

                    mem_info = self.machine.membership_list.active_nodes[host]
                    heartbeat_increase_time = mem_info.heartbeat_increase_time

                    time_now = datetime.datetime.now()
                    if (time_now - heartbeat_increase_time).total_seconds() > T_SUSPECT:

                        if self.machine.membership_list.active_nodes[host].suspicion["flag"] == 0:

                            with self.membership_mutex:
                                self.machine.membership_list.active_nodes[host].suspicion["flag"] = 1
                                self.machine.membership_list.active_nodes[host].suspicion["start_time"] = time_now

                            print(f'Node {host[0]}:{host[1]} has been suspected at {datetime.datetime.now()} \n')
                            self.machine.logger.debug(f'Node {host} has been suspected')

            time.sleep(0.2)


    def fail(self):
        ''' Fail a member from the membership list if 
            1. It is suspected for T_FAIL seconds
            2. No heartbeat was received for T_FAIL seconds
         '''
        while True:
            if self.SUSPICION_RECVD:
                continue

            active_nodes = list(self.machine.membership_list.active_nodes.keys()).copy()
            for host in active_nodes:

                mem_info = self.machine.membership_list.active_nodes[host]
                time_now = datetime.datetime.now()

                # If GOSSIP + S is enabled, then check if the node is suspected for T_FAIL seconds
                if self.ENABLE_SUSPICION:
                    if mem_info.suspicion["flag"] == 1:
                        suspicion_start_time = mem_info.suspicion["start_time"]

                        if (time_now - suspicion_start_time).total_seconds() > T_FAIL:
                            if self.machine.membership_list.cleanup_status_dict[host]["flag"] == 0:

                                with self.membership_mutex:
                                    self.machine.membership_list.cleanup_status_dict[host]["flag"] = 1
                                    self.machine.membership_list.cleanup_status_dict[host]["cleanup_start_time"] = time_now
                                    self.machine.membership_list.failed_nodes.append(host)

                                print(f'[By Itself] Node {host[0]}:{host[1]} has been failed {datetime.datetime.now()} \n')
                                self.machine.logger.debug(f'Node {host} has been failed')

                # If GOSSIP, then check if the node has not sent a heartbeat for T_FAIL seconds
                else:
                    heartbeat_increase_time = mem_info.heartbeat_increase_time

                    if (time_now - heartbeat_increase_time).total_seconds() > T_FAIL:
                        if self.machine.membership_list.cleanup_status_dict[host]["flag"] == 0:

                            with self.membership_mutex:
                                self.machine.membership_list.cleanup_status_dict[host]["flag"] = 1
                                self.machine.membership_list.cleanup_status_dict[host]["cleanup_start_time"] = time_now
                                self.machine.membership_list.failed_nodes.append(host)

                            print(f'[By itself] Node {host[0]}:{host[1]} has been failed at {datetime.datetime.now()} \n')
                            self.machine.logger.debug(f'Node {host} has been failed')

            time.sleep(0.2)

        
    def receive(self):
        ''' Receive a message from another machine '''
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # IPv4, UDP
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.port))

        while True:
            data, server = sock_fd.recvfrom(MAX) # receive serialized data from another machine
            
            if self.machine.status == 'Joined':
                if data:
                    mssg = pickle.loads(data)

                    if mssg.type == 'ping':
                        u = random.uniform(0, 1)
                        if u > MESSAGE_DROP_RATE:
                            with self.membership_mutex:
                                    self.update_membership_list(mssg)

                    if mssg.type == 'join':
                        with self.membership_mutex:
                            self.add_membership_list(mssg)

                    if mssg.type == 'leave':
                        with self.membership_mutex:
                            self.remove_membership_list(mssg)

                    if mssg.type == 'suspicion':
                        self.SUSPICION_RECVD = True
                        print("\nSwitching to GOSSIP + S ...")

                        self.ENABLE_SUSPICION = True
                        time.sleep(5)

                        # Change the heartbeat increase time to current time for all nodes
                        with self.membership_mutex:
                            for host in self.machine.membership_list.active_nodes.keys():
                                self.machine.membership_list.active_nodes[host].heartbeat_increase_time = datetime.datetime.now()
                                                  
                        self.SUSPICION_RECVD = False
                        print("*** GOSSIP + S Enabled ***\n")

                    if mssg.type == 'gossip':
                        self.SUSPICION_RECVD = True
                        print("\nSwitching to GOSSIP ...")

                        self.ENABLE_SUSPICION = False
                        time.sleep(5)

                        # Change the heartbeat increase time to current time for all nodes
                        with self.membership_mutex:
                            for host in self.machine.membership_list.active_nodes.keys():
                                self.machine.membership_list.active_nodes[host].heartbeat_increase_time = datetime.datetime.now()
                                                  
                        self.SUSPICION_RECVD = False
                        print("*** GOSSIP Enabled ***\n")
        
        sock_fd.close()


    def server(self):
        ''' Start the server '''
        print("Starting server thread for failure detection")
        fail_thread = threading.Thread(target=self.fail)
        suspect_thread = threading.Thread(target=self.suspect)
        remove_thread = threading.Thread(target=self.remove_member)
        receiver_thread = threading.Thread(target=self.receive)

        fail_thread.start()
        suspect_thread.start()
        remove_thread.start()
        receiver_thread.start()
        fail_thread.join()
        suspect_thread.join()
        remove_thread.join()
        receiver_thread.join()


    def gossip_message(self, msg, sock_fd=None):
        ''' Gossip a message to B random nodes from the membership list '''
        GOSSIP_NODES = []
        for mem_host in self.machine.membership_list.active_nodes.keys():
            if mem_host != self.machine.nodeId:
                if self.machine.membership_list.cleanup_status_dict[mem_host]["flag"] == 0:
                    GOSSIP_NODES.append((mem_host[0], mem_host[1]))

        gossip_neighbours = random.sample(GOSSIP_NODES, B) if B < len(GOSSIP_NODES) else GOSSIP_NODES
        self.machine.logger.info(f'{self.machine.nodeId[1]} Gossiping to {gossip_neighbours}')
        for neighbour in gossip_neighbours:
            gossip_ip = neighbour[0]
            gossip_port = neighbour[1]

            # print(f"Sending Ping to {gossip_ip}:{gossip_port}, with counter {msg.counter}")
            self.send_message(sock_fd, pickle.dumps(msg), gossip_ip, gossip_port)
        

    def ping(self, sock_fd=None):
        ''' Send a ping message to the nodes in the membership list.
            Also update the membership list of the current machine with the counter
        '''
        while True:
            if self.SUSPICION_RECVD:
                continue

            if self.machine.status == 'Joined':
                self.ping_counter += 1
                
                # Update your own membership list
                if self.machine.nodeId not in self.machine.membership_list.active_nodes.keys():
                    self.machine.membership_list.add_member(host=self.machine.nodeId, 
                                                    membership_counter=self.ping_counter, 
                                                    suspicion={"flag":0, "start_time":None}
                                                    )
                else:
                    self.machine.membership_list.update_member(host=self.machine.nodeId, 
                                                        membership_counter=self.ping_counter, 
                                                        cleanup_status=None,
                                                        logger=self.machine.logger
                                                        )  
                
                
                msg = Message(msg_type='ping', 
                              host=self.machine.nodeId, 
                              membership_list=self.machine.membership_list, 
                              counter=self.ping_counter
                             )
                self.gossip_message(msg, sock_fd)
                time.sleep(T_GOSSIP)
    

    def node_join(self):
        # Add the current machine to its own membership list and send message to the introducer
        self.version = time.mktime(datetime.datetime.now().timetuple())
        host = (self.ip, self.port, self.version, self.MACHINE_NUM)
        self.machine.nodeId = host

        with self.membership_mutex:
            if host not in self.machine.membership_list.active_nodes.keys():
                self.machine.membership_list.add_member(host=host, 
                                                membership_counter=self.ping_counter,
                                                suspicion={"flag":0, "start_time":None}
                                                )

        msg = Message(msg_type='join', host=host, membership_list=None, counter=self.ping_counter)
        with self.machine.status_mutex:
            self.machine.status = 'Joined'
        self.send_message(self.sock_fd, pickle.dumps(msg), self.introducer_ip, self.introducer_port)
        
        return 1


    def list_mem(self):
        ''' List the membership List '''
        print_list = []
        print("Listing membership list")
        for k, v in self.machine.membership_list.active_nodes.items():
            print_list.append("Node ID: {} ----- Counter: {}".format(k, v.membership_counter))
        
        print("Membership List: ")
        print("-----------------")
        print(*print_list, sep="\n")
        print("\n")


    def list_self(self):
        ''' List Self Node ID '''
        print(self.machine.nodeId)
        print("\n")


    def enable_suspicion(self):
        ''' Enable GOSSIP + S '''
        self.SUSPICION_RECVD = True
        print("\nSwitching to GOSSIP + S ...")

        for neighbour in self.machine.membership_list.active_nodes.keys():
            if neighbour == self.machine.nodeId:
                continue
            multicast_ip = neighbour[0]
            multicast_port = neighbour[1]

            host = (self.ip, self.port, self.version)

            msg = Message('suspicion', host, None, None)
            self.send_message(self.sock_fd, pickle.dumps(msg), multicast_ip, multicast_port)

        self.ENABLE_SUSPICION = True
        time.sleep(5)

        # Change the heartbeat increase time to current time for all nodes
        with self.membership_mutex:
            for host in self.machine.membership_list.active_nodes.keys():
                self.machine.membership_list.active_nodes[host].heartbeat_increase_time = datetime.datetime.now()

        self.SUSPICION_RECVD = False
        print("*** GOSSIP + S Enabled ***\n")



    def disable_suspicion(self):
        ''' Enable GOSSIP '''
        self.SUSPICION_RECVD = True
        print("\nSwitching to GOSSIP ...")

        for neighbour in self.machine.membership_list.active_nodes.keys():
            if neighbour == self.machine.nodeId:
                continue
            multicast_ip = neighbour[0]
            multicast_port = neighbour[1]

            host = (self.ip, self.port, self.version)

            msg = Message('gossip', host, None, None)
            self.send_message(self.sock_fd, pickle.dumps(msg), multicast_ip, multicast_port)

        self.ENABLE_SUSPICION = False
        time.sleep(5)

        # Change the heartbeat increase time to current time for all nodes
        with self.membership_mutex:
            for host in self.machine.membership_list.active_nodes.keys():
                self.machine.membership_list.active_nodes[host].heartbeat_increase_time = datetime.datetime.now()

        self.SUSPICION_RECVD = False
        print("*** GOSSIP Enabled ***\n")


    """
    def command(self, sock_fd):
        ''' Send a command to the machine '''
        INTRODUCER = NODES[0]
        machine_num = INTRODUCER[0]
        introducer_ip = socket.gethostbyname(INTRODUCER[1])
        introducer_port = BASE_PORT + machine_num

        while True:
            inp = input()
 
            if inp == "list_mem":
                print_list = []
                for k, v in self.machine.membership_list.active_nodes.items():
                    print_list.append("Node ID: {} ----- Counter: {}".format(k, v.membership_counter))
                
                print("Membership List: ")
                print("-----------------")
                print(*print_list, sep="\n")
                print("\n")


            elif inp == "list_self":
                print(self.machine.nodeId)
                print("\n")


            elif inp == "join":
                # Add the current machine to its own membership list and send message to the introducer
                self.version = time.mktime(datetime.datetime.now().timetuple())
                host = (self.ip, self.port, self.version)
                self.machine.nodeId = host

                with self.membership_mutex:
                    if host not in self.machine.membership_list.active_nodes.keys():
                        self.machine.membership_list.add_member(host=host, 
                                                        membership_counter=self.ping_counter,
                                                        suspicion={"flag":0, "start_time":None}
                                                        )

                msg = Message(msg_type='join', host=host, membership_list=None, counter=self.ping_counter)

                with self.machine.status_mutex:
                    self.machine.status = 'Joined'
                
                self.send_message(sock_fd, pickle.dumps(msg), introducer_ip, introducer_port)


            elif inp == "leave":
                with self.membership_mutex:

                    host = (self.ip, self.port, self.version)
                    with self.machine.status_mutex:
                        self.machine.status = 'Not Joined'

                    host_list = list(self.machine.membership_list.active_nodes.keys())
                    for active_host in host_list:
                        self.machine.membership_list.delete_member(active_host)
                
                    msg = Message(msg_type='leave', host=host, membership_list=None, counter=self.ping_counter)
                    self.send_message(sock_fd, pickle.dumps(msg), introducer_ip, introducer_port)


            elif inp == "enable suspicion":
                self.SUSPICION_RECVD = True
                print("\nSwitching to GOSSIP + S ...")

                for neighbour in self.machine.membership_list.active_nodes.keys():
                    if neighbour == self.machine.nodeId:
                        continue
                    multicast_ip = neighbour[0]
                    multicast_port = neighbour[1]

                    host = (self.ip, self.port, self.version)

                    msg = Message('suspicion', host, None, None)
                    self.send_message(sock_fd, pickle.dumps(msg), multicast_ip, multicast_port)

                self.ENABLE_SUSPICION = True
                time.sleep(5)

                # Change the heartbeat increase time to current time for all nodes
                with self.membership_mutex:
                    for host in self.machine.membership_list.active_nodes.keys():
                        self.machine.membership_list.active_nodes[host].heartbeat_increase_time = datetime.datetime.now()

                self.SUSPICION_RECVD = False
                print("*** GOSSIP + S Enabled ***\n")

            
            elif inp == "disable suspicion":
                self.SUSPICION_RECVD = True
                print("\nSwitching to GOSSIP ...")

                for neighbour in self.machine.membership_list.active_nodes.keys():
                    if neighbour == self.machine.nodeId:
                        continue
                    multicast_ip = neighbour[0]
                    multicast_port = neighbour[1]

                    host = (self.ip, self.port, self.version)

                    msg = Message('gossip', host, None, None)
                    self.send_message(sock_fd, pickle.dumps(msg), multicast_ip, multicast_port)

                self.ENABLE_SUSPICION = False
                time.sleep(5)

                # Change the heartbeat increase time to current time for all nodes
                with self.membership_mutex:
                    for host in self.machine.membership_list.active_nodes.keys():
                        self.machine.membership_list.active_nodes[host].heartbeat_increase_time = datetime.datetime.now()

                self.SUSPICION_RECVD = False
                print("*** GOSSIP Enabled ***\n")


            else:
                print("Input not recognized. It must be either join/leave.. Enter again")
    """            

    def client(self):
        ''' Start the client '''
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        ping_thread = threading.Thread(target=self.ping, args=(sock_fd,))
        # command_thread = threading.Thread(target=self.command, args=(sock_fd,))

        ping_thread.start()
        # command_thread.start()
        ping_thread.join()
        # command_thread.join()
    

    def start_machine(self):
        server_thread = threading.Thread(target=self.server)
        client_thread = threading.Thread(target=self.client)

        server_thread.start()
        client_thread.start()
        # server_thread.join()
        # client_thread.join()



# if __name__ == "__main__":
#     MACHINE_NUM = sys.argv[1]

#     machine = Machine(int(MACHINE_NUM))
#     machine.start_machine()