import socket
import sys
import threading
import pickle
import random
import datetime
import time
import os
from utils import *
from static import *
import logging
import time
import handler
from machine import Machine
from failure_detector import Failure_Detector
from file_system import File_System

OFFSET = 0
MAX = 8192                  # Max size of message
INIT_STATUS = 'Not Joined'  # Initial status of a node
BASE_FS_PORT = 9000 + OFFSET
BASE_PORT = 8000 + OFFSET
RAND_PORT = 57757


class Client:

    def __init__(self, MACHINE_NUM, STATUS=INIT_STATUS):
        self.MACHINE_NUM = MACHINE_NUM
        self.port = BASE_PORT + MACHINE_NUM
        self.hostname = "fa23-cs425-37" + f"{MACHINE_NUM:02d}" + ".cs.illinois.edu"
        self.ip = socket.gethostbyname(self.hostname)
        self.machine = Machine()

        if self.MACHINE_NUM == 1:
            self.version = time.mktime(datetime.datetime.now().timetuple())
            self.machine.nodeId = (self.ip, self.port, self.version, self.MACHINE_NUM)

        logging.basicConfig(filename=f"vm{self.MACHINE_NUM}.log",
                                        filemode='w',
                                        format='[%(asctime)s | %(levelname)s]: %(message)s',
                                        level=logging.DEBUG)
        self.machine.logger = logging.getLogger(f'vm{self.MACHINE_NUM}.log')

        self.machine.status = 'Joined' if MACHINE_NUM==1 else STATUS
        self.machine.membership_list = MembershipList()  
        self.put_start_time = 0
        self.put_end_time = 0
        self.get_start_time = 0
        self.get_end_time = 0


    def send_message(self, sock_fd, msg):
        ''' Send a message to another machine '''
        try:
            sock_fd.sendall(msg)
        except:
            pass


    def server(self):
        ''' Start the Failure Detector and File System '''
        self.fail_detector = Failure_Detector(self.MACHINE_NUM, self.machine)
        self.fail_detector.start_machine()

        time.sleep(4)
        self.file_system = File_System(self.MACHINE_NUM, self.machine)
        self.file_system.start_machine()
    


    def read_replicas(self, mssg, sdfs_filename, local_filename):
        ''' Read a file from one of the replica '''
        replicas = mssg.kwargs['replica']
        for replica in replicas:
            try:
                sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock_fd.connect((replica[0], replica[1]))
                break 
            except:
                continue

        # Send filename message to the replicas
        self.send_message(sock_fd, pickle.dumps(sdfs_filename))

        with open(local_filename, 'wb') as f:
            bytes_read = sock_fd.recv(self.machine.BUFFER_SIZE)
            while bytes_read:
                if not bytes_read:
                    break
                else:
                    # write to the file the bytes we just received
                    f.write(bytes_read)
                    bytes_read = sock_fd.recv(self.machine.BUFFER_SIZE)
        
        sock_fd.close()
        self.get_end_time = datetime.datetime.now()
        print("[ACK Received] Get file successfully")
        print(f"Total Time Taken: {(self.get_end_time - self.get_start_time).total_seconds()}\n")


    def handle_get(self, sdfs_filename, local_filename):
        ''' Get a file from the SDFS '''
        leader_node = self.file_system.get_leader_node()

        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((leader_node[0], leader_node[1]))   

        get_mssg = Message(msg_type="get", 
                        host=self.machine.nodeId,
                        membership_list=None,
                        counter=None,
                        filename=sdfs_filename,
                        port=RAND_PORT
                        )
        # Send get message to the leader
        self.send_message(sock_fd, pickle.dumps(get_mssg))
        self.machine.logger.info("Get Message sent")
        sock_fd.close()
            
        recv_sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        recv_sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        recv_sock_fd.bind((self.ip, RAND_PORT))

        recv_sock_fd.listen(1)
        conn, addr = recv_sock_fd.accept()
        data = conn.recv(MAX)
        mssg = pickle.loads(data)
        conn.close()

        if mssg.type == "replica":
            self.machine.logger.info(f"Replica Servers: {mssg.kwargs['replica']}")
            # Read the file from one of the replicas
            self.read_replicas(mssg, sdfs_filename, local_filename)
        elif mssg.type == "NACK":
            print("[ACK not Received] File not found in SDFS\n")
        else:
            print("Unsuccessful Attempt")


    
    def handle_multiread(self, sdfs_filename, local_filename, machines):
        ''' Perform a multiread from the SDFS '''
        leader_node = self.file_system.get_leader_node()

        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((leader_node[0], leader_node[1]))

        mssg = Message(msg_type="multiread",
                        host=self.machine.nodeId,
                       membership_list=None,
                        counter=None,
                        sdfs_filename=sdfs_filename,
                        local_filename=local_filename,
                        machines=machines,
                        )
        self.send_message(sock_fd, pickle.dumps(mssg))
        self.machine.logger.info('Multiread Message sent to Leader')

        machine_nums = [int(machine.split('VM')[1]) for machine in machines]
        if self.machine.nodeId[3] in machine_nums:
            data = sock_fd.recv(MAX)
            mssg = pickle.loads(data)
            sock_fd.close()

            if mssg.type == "replica":
                self.machine.logger.info(f"Replica Servers: {mssg.kwargs['replica']}")
                self.read_replicas(mssg, sdfs_filename, local_filename)
            else:
                print("Unsuccessful Attempt")

        else:
            print("Multiread sent to Respective Machines")
            sock_fd.close()



    def delete_replicas(self, mssg, sdfs_filename):
        replica_servers = mssg.kwargs['replica']
        ack_count = 0
        for replica in replica_servers:
            sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock_fd.connect((replica[0], replica[1])) 

            # Send filename message to the replicas
            self.send_message(sock_fd, pickle.dumps(sdfs_filename))
            data = sock_fd.recv(MAX)
            msg = pickle.loads(data)

            if msg.type == "ACK":
                ack_count += 1

            sock_fd.close()
        
        if ack_count == self.machine.REPLICATION_FACTOR:
            print("[ACK Received] Deleted file successfully\n")
        else:
            print("[ACK Not Received] Deleted file unsuccessfully\n")
    


    def handle_delete(self, sdfs_filename):
        ''' Delete a file from the SDFS '''
        leader_node = self.file_system.get_leader_node()

        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((leader_node[0], leader_node[1])) 

        delete_mssg = Message(msg_type="delete", 
                        host=self.machine.nodeId,
                        membership_list=None,
                        counter=None,
                        filename=sdfs_filename,
                        )                      

        self.send_message(sock_fd, pickle.dumps(delete_mssg))
        data = sock_fd.recv(MAX)
        mssg = pickle.loads(data)
        sock_fd.close()

        if mssg.type == "replica":
            self.machine.logger.info(f"Replica Servers: {mssg.kwargs['replica']}")
            self.delete_replicas(mssg, sdfs_filename)
        elif mssg.type == "NACK":
            print("[ACK not Received] File not found in SDFS\n")



    def handle_maple(self, tokens):
        leader_node = self.file_system.get_leader_node()

        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((leader_node[0], leader_node[1]))

        maple_mssg = Message(msg_type="maple",
                            host=self.machine.nodeId,
                            membership_list=None,
                            counter=None,
                            maple_exe=tokens[1],
                            num_maples=tokens[2],
                            sdfs_prefix=tokens[3],
                            input_files=tokens[4],
                            query=None,
                            )
        
        self.send_message(sock_fd, pickle.dumps(maple_mssg))
        self.machine.logger.info('Maple Message sent to Leader')
        sock_fd.close()
    

    
    def handle_juice(self, tokens):
        leader_node = self.file_system.get_leader_node()

        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((leader_node[0], leader_node[1]))

        maple_mssg = Message(msg_type="juice",
                            host=self.machine.nodeId,
                            membership_list=None,
                            counter=None,
                            juice_exe=tokens[1],
                            num_juices=tokens[2],
                            sdfs_prefix=tokens[3],
                            dest_filename=tokens[4],
                            delete_intermediate=tokens[5],
                            )
        
        self.send_message(sock_fd, pickle.dumps(maple_mssg))
        self.machine.logger.info('Juice Message sent to Leader')
        sock_fd.close()


    def handle_demo1(self, tokens, query):
        leader_node = self.file_system.get_leader_node()

        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((leader_node[0], leader_node[1]))

        maple_mssg = Message(msg_type="demo1",
                            host=self.machine.nodeId,
                            membership_list=None,
                            counter=None,
                            maple1_exe=tokens[1],
                            juice1_exe=tokens[2],
                            maple2_exe=tokens[3],
                            juice2_exe=tokens[4],
                            num_maples=tokens[5],
                            num_juices=tokens[6],
                            sdfs_prefix=tokens[7],
                            input_files=tokens[8],
                            dest_filename=tokens[9],
                            query=query,
                            )
        
        self.send_message(sock_fd, pickle.dumps(maple_mssg))
        self.machine.logger.info('Juice Message sent to Leader')
        sock_fd.close()


    def handle_maplejuice(self, tokens, query):
        leader_node = self.file_system.get_leader_node()

        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((leader_node[0], leader_node[1]))

        maplejuice_mssg = Message(msg_type="maplejuice",
                            host=self.machine.nodeId,
                            membership_list=None,
                            counter=None,
                            maple_exe=tokens[0],
                            juice_exe=tokens[1],
                            num_maples=tokens[2],
                            num_juices=tokens[3],
                            sdfs_prefix=tokens[4],
                            input_files=tokens[5],
                            dest_filename=tokens[6],
                            delete_intermediate=tokens[7],
                            query=query,
                            )
        
        self.send_message(sock_fd, pickle.dumps(maplejuice_mssg))
        self.machine.logger.info('Maplejuice Message sent to Leader')
        sock_fd.close()
    


    def client(self):
        ''' Start the client '''
        while True:
            inp = input()

            if inp == "list_mem":
                self.fail_detector.list_mem()

            elif inp == "list_self":
                self.fail_detector.list_self()

            elif inp == "join":
                self.fail_detector.node_join()

            elif inp == "enable suspicion":
                self.fail_detector.enable_suspicion()

            elif inp == "disable suspicion":
                self.fail_detector.disable_suspicion()

            elif inp.startswith("put"):
                # self.put_start_time = datetime.datetime.now()
                _, local_filename, sdfs_filename = inp.split(' ')
                leader_node = self.file_system.get_leader_node()
                handler.handle_put(self.machine, leader_node, self.ip, 'wb', local_filename, sdfs_filename)

            elif inp.startswith("get"):
                self.get_start_time = datetime.datetime.now()
                _, sdfs_filename, local_filename = inp.split(' ')
                self.handle_get(sdfs_filename, local_filename)

            elif inp.startswith("multiread"):
                self.get_start_time = datetime.datetime.now()
                tokens = inp.split(' ')
                sdfs_filename = tokens[1]
                local_filename = tokens[2]
                machines = tokens[3:]
                self.handle_multiread(sdfs_filename, local_filename, machines)

            elif inp.startswith("delete"):
                _, sdfs_filename = inp.split(' ')
                leader_node = self.file_system.get_leader_node()
                handler.handle_delete(self.machine, leader_node, sdfs_filename)
            
            elif inp.startswith("ls"): # list all machines where file is being stored
                _, sdfs_filename = inp.split(' ')
                self.file_system.ls_sdfsfilename(sdfs_filename)
            
            elif inp == "store": # list all files being stored in current machine
                self.file_system.store("./DS")

            elif inp.startswith("maple"):
                tokens = inp.split(' ')
                self.handle_maple(tokens)

            elif inp.startswith("juice"):
                tokens = inp.split(' ')
                self.handle_juice(tokens)
            
            elif inp.startswith("sql"): # sql 
                tokens = inp.split('"')
                query = tokens[1]
                maplejuice_tokens = tokens[-1].split(' ')
                maplejuice_tokens = list(filter(('').__ne__, maplejuice_tokens))
                print(maplejuice_tokens)
                self.handle_maplejuice(maplejuice_tokens, query)

            elif inp.startswith("demo1"):
                
                tokens = inp.split('"')
                print(tokens)
                query = tokens[-2]
                new_tokens = tokens[0].split(' ')
                self.handle_demo1(new_tokens, query)
            
            elif inp == "list_replica_dict":
                self.file_system.list_replica_dict()

            elif inp == "list_failed_nodes":
                self.file_system.list_failed_nodes()

            elif inp == "print_leader":
                self.file_system.print_leader()
            
            elif inp == "write_wikicorpus":
                file_paths = []
                for name in os.listdir("../WikiCorpus/"):
                    path = os.path.join("../WikiCorpus/", name)
                    file_paths.append(path)
                
                start_time = datetime.datetime.now()
                print(f"{len(file_paths)} in total")
                for i, fpath in enumerate(sorted(file_paths)):
                    self.put_start_time = datetime.datetime.now()

                    fname = os.path.basename(fpath)
                    self.put(fpath, fname)
                    print(f"{i} done - {fname}")
                
                    end_time = datetime.datetime.now()
                    print("Time elapsed so far: {}\n".format((end_time - start_time).total_seconds()))


    def start_machine(self):
        print(f"Machine {self.MACHINE_NUM} Running, Status: {self.machine.status}")

        server_thread = threading.Thread(target=self.server)
        client_thread = threading.Thread(target=self.client)
        server_thread.start()
        client_thread.start()
        server_thread.join()
        client_thread.join()


# if __name__ == "__main__":
#     MACHINE_NUM = sys.argv[1]
#     print(MACHINE_NUM)

#     machine = Machine(int(MACHINE_NUM))
#     machine.start_machine()