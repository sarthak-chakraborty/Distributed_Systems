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
import os
import shutil
import handler
from collections import defaultdict
from collections import Counter
import copy

# Static variables
OFFSET = 0
BASE_PORT = 8000 + OFFSET
BASE_FS_PORT = 9000 + OFFSET
BASE_WRITE_PORT = 10000 + OFFSET
BASE_READ_PORT = 11000 + OFFSET
BASE_DELETE_PORT = 12000 + OFFSET
BASE_FS_PING_PORT = 13000 + OFFSET
BASE_REREPLICATION_PORT = 14000 + OFFSET
BASE_MAPLEJUICE_PORT = 15000 + OFFSET
MAX = 8192
T_WAIT = 12
T_MAPLEJUICE_FAIL_WAIT = 32

HOME_DIR = "./DS"


class File_System:

    def __init__(self, MACHINE_NUM, MACHINE):
        self.MACHINE_NUM = MACHINE_NUM
        self.port = BASE_FS_PORT + MACHINE_NUM
        self.write_port = BASE_WRITE_PORT + MACHINE_NUM
        self.read_port = BASE_READ_PORT + MACHINE_NUM
        self.delete_port = BASE_DELETE_PORT + MACHINE_NUM
        self.fs_ping_port = BASE_FS_PING_PORT + MACHINE_NUM
        self.rereplication_port = BASE_REREPLICATION_PORT + MACHINE_NUM
        self.maplejuice_port = BASE_MAPLEJUICE_PORT + MACHINE_NUM
        self.hostname = "fa23-cs425-37" + f"{MACHINE_NUM:02d}" + ".cs.illinois.edu"
        self.ip = socket.gethostbyname(self.hostname)
        self.machine = MACHINE

        self.leader_node = None
        self.write_queue = []
        self.read_queue = []
        self.op_timestamps = []

        self.task_queue = []
        self.maple_juice_queue = []
        self.maple_tasks_completed = 0
        self.juice_tasks_completed = 0

        try:
            shutil.rmtree(HOME_DIR)
        except:
            pass
        os.mkdir(HOME_DIR)


    def send_message(self, sock_fd, msg):
        ''' Send a message to another machine '''
        try:
            sock_fd.sendall(msg)
        except:
            pass


    def store(self, directory):
        for filename in os.listdir(directory):
            path = os.path.join(directory, filename)
            if os.path.isfile(path):
                size = os.path.getsize(path)
                print(f"File: {filename}, Size: {size} bytes")
        print("\n")


    def list_replica_dict(self):
        print(self.machine.membership_list.file_replication_dict.items())
        print("\n")


    def list_failed_nodes(self):
        print(self.machine.membership_list.failed_nodes)
        print("\n")

    
    def print_leader(self):
        print(self.leader_node)
        print("\n")


    def ls_sdfsfilename(self, sdfsfilename):
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((self.leader_node[0], BASE_FS_PORT  + self.leader_node[3]))

        mssg = Message(msg_type='ls',
                        host=self.machine.nodeId,
                        membership_list=None,
                        counter=None,
                        sdfsfilename=sdfsfilename
                        )
        self.send_message(sock_fd, pickle.dumps(mssg))
        data = sock_fd.recv(MAX)
        mssg = pickle.loads(data)
        sock_fd.close()

        if mssg.type == "replica":
            print("------Replicas:------")
            for replica in mssg.kwargs['replica']:
                print(f"Machine Num: {replica[3]}, IP: {replica[0]}")
        else:
            print("File not found")
        print("\n")


    def get_leader_node(self):
        return self.leader_node


    def argmax(self, a):
        return max(range(len(a)), key=lambda x: a[x])


    def leader_election(self, sock_fd=None):
        ''' Detect whether the leader has failed 
            If the leader has failed, start a new election
        '''
        while True:
            if self.machine.status == "Joined":
                machines = list(self.machine.membership_list.active_nodes.keys())
                machine_ids = [machine[1] for machine in machines]
                max_machine_id_index = self.argmax(machine_ids)

                leader_node = machines[max_machine_id_index]
                self.leader_node = (leader_node[0], leader_node[1] - BASE_PORT + BASE_FS_PORT, leader_node[2], leader_node[3])
                self.machine.logger.info(f"New leader elected: {self.leader_node}")

                # TODO: Send a message to all machines to update the leader
                # host = (self.ip, self.port, self.version)
                # msg = Message(msg_type='leader_update', 
                #               host=host, 
                #               membership_list=self.machine.membership_list, 
                #               counter=self.ping_counter
                #              )
                # self.send_leader_msg(msg, sock_fd)

                # TODO: For the failed node, check the replicas stored and re-replicate the files
                time.sleep(4)


    def filestore_ping_recv(self):
        ''' Receive a ping message at leader and update the file replication list '''
        while True:
            if self.machine.status == "Joined" and self.leader_node is not None:
                if self.machine.nodeId[3] == self.leader_node[3]:
                    sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock_fd.bind((self.ip, self.fs_ping_port))
                    sock_fd.listen(5)

                    while self.machine.nodeId[3] == self.leader_node[3]:
                        self.machine.logger.debug(f"File Replication Dict: {self.machine.membership_list.file_replication_dict}")

                        conn, addr = sock_fd.accept()
                        try:
                            data = conn.recv(MAX)   # receive serialized data from another machine
                            mssg = pickle.loads(data)

                            if mssg.type == 'write_ping':
                                file = mssg.kwargs['replica']
                                if file not in self.machine.membership_list.file_replication_dict:
                                    self.machine.membership_list.file_replication_dict[file] = {mssg.host}
                                else:
                                    self.machine.membership_list.file_replication_dict[file].add(mssg.host)
                                
                                if len(self.machine.membership_list.file_replication_dict[file]) == self.machine.REPLICATION_FACTOR: # should be min(num_machines, replication factor)
                                    self.machine.membership_list.write_lock_set.remove(file)
                            
                            elif mssg.type == 'read_ping':
                                file = mssg.kwargs['replica']
                                self.machine.membership_list.read_lock_dict[file] -= 1
                            
                            elif mssg.type == 'delete_ping':
                                file = mssg.kwargs['replica']
                                if file in self.machine.membership_list.file_replication_dict:
                                    self.machine.membership_list.file_replication_dict[file].remove(mssg.host)
                                    # Removing from file replication dict is there is no replica for the file
                                    if len(self.machine.membership_list.file_replication_dict[file]) == 0:
                                        self.machine.membership_list.file_replication_dict.pop(file)
                                        self.machine.membership_list.write_lock_set.remove(file)

                            elif mssg.type == 'maple_ping':
                                file = mssg.kwargs['replica']
                                # Check key from a given value in a dictionary
                                worker = None
                                for k,v in self.machine.membership_list.task_running_workers.items():
                                    if v in file:
                                        worker = k
                                        break
                                print("Maple Task Finished: ", file)
                                self.maple_tasks_completed -= 1
                                self.machine.membership_list.task_running_workers.pop(worker)

                            elif mssg.type == 'juice_ping':
                                file = mssg.kwargs['replica']
                                # Check key from a given value in a dictionary
                                worker = None
                                for k,v in self.machine.membership_list.task_running_workers.items():
                                    if v in file:
                                        worker = k
                                        break
                                print("Juice Task Finished: ", file)
                                self.juice_tasks_completed -= 1
                                self.machine.membership_list.task_running_workers.pop(worker)
                                
                                # if len(self.machine.membership_list.file_replication_dict[file]) == 0: # TODO: should the key even exist?
                                #     self.machine.membership_list.write_lock_set.remove(file)

                        except Exception as e:
                            self.machine.logger.error(f"Error in receiving filestore ping: {e}")

                        finally:
                            conn.close()
                    sock_fd.close()

    

    def filestore_ping(self, op_type, replica):
        ''' Send a ping message to leader about the replicas stored '''
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((self.leader_node[0], BASE_FS_PING_PORT  + self.leader_node[3])) 

        if op_type == 'w':
            msg_type = 'write_ping'
        elif op_type == 'r':
            msg_type = 'read_ping'
        elif op_type == 'd':
            msg_type = 'delete_ping'
        elif op_type == 'm':
            msg_type = 'maple_ping'
        elif op_type == 'j':
            msg_type = 'juice_ping'
        else:
            msg_type = None

        msg = Message(msg_type=msg_type, 
                    host=self.machine.nodeId, 
                    membership_list=None, 
                    counter=None,
                    replica=replica
                    )
        self.send_message(sock_fd, pickle.dumps(msg))
        sock_fd.close()



    def rereplication_leader_helper(self, filename, node, results, index):
        ''' Find a node where the replica will be stored and ask another current replica node to send the file to the new chosen node '''
        alive_replica_node = None
        new_replica_node = None

        # Find the new node where the replica will be stored
        while True:
            new_replica_node = random.sample(self.machine.membership_list.active_nodes.keys(), 1)[0]
            replica_nodes = self.machine.membership_list.file_replication_dict[filename]
            if new_replica_node not in replica_nodes:
                break

            try:
                print("trying to rereplication_leader_helper")
                # Find a node from where the content will be copied to the new node
                replica_nodes = self.machine.membership_list.file_replication_dict[filename]
                active_nodes = list(self.machine.membership_list.active_nodes.keys())
                all_replica_nodes = list(set(replica_nodes).intersection(active_nodes))
                alive_replica_node = list(all_replica_nodes)[0]

                sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                # print("[Re-Replication] Sending message to {}: Instr copy replica {} from {} to {} ".format(alive_replica_node, filename, alive_replica_node, new_replica_node))
                print("Trying to connect to socket")
                sock_fd.connect((alive_replica_node[0], BASE_REREPLICATION_PORT  + alive_replica_node[3]))
                print("Connected to socket")

                mssg = Message(msg_type='put_replica',
                                host=self.machine.nodeId,
                                membership_list=None,
                                counter=None,
                                filename=filename,
                                replica_node=new_replica_node
                                )
                self.machine.logger.info("[Re-Replication] Sending message to {}: Instr copy replica {} from {} to {} ".format(alive_replica_node, filename, alive_replica_node, new_replica_node))
                self.send_message(sock_fd, pickle.dumps(mssg))
                # Wait for ACK
                print("Waiting for ack from {}", alive_replica_node)
                data = sock_fd.recv(MAX)
                print("Received ack")
                mssg = pickle.loads(data)
                sock_fd.close()

                if mssg.type == "ACK":
                    self.machine.membership_list.file_replication_dict[filename].add(new_replica_node)
                    self.machine.membership_list.file_replication_dict[filename].remove(node)
                    results[index] = 1
                else:
                    # TODO: What if rereplication failed, what to do?
                    pass

                print("rereplication_leader_helper done")
                break
            except:
                print("Error in rereplication_leader_helper...looping again")


    def rereplication_leader(self):
        ''' Re-replicate the files stored in the failed node '''
        while True:
            if (self.machine.status == "Joined") and \
                (self.leader_node is not None): # leader must be a part of file replication dict
                if self.machine.nodeId[3] == self.leader_node[3]:
                    if len(self.machine.membership_list.failed_nodes) > 0:
                        # print(f"Node Failed at: {datetime.datetime.now()}")
                        print("Files to be Re-replicated")
                        time.sleep(T_WAIT)
                        print(f"Rereplication Start: {datetime.datetime.now()}")
                        # print("Failed Nodes Currently: ", self.machine.membership_list.failed_nodes)
                        node = self.machine.membership_list.failed_nodes[0]

                        d = self.machine.membership_list.file_replication_dict
                        inverted_replica_dict = defaultdict(list) # dict from machine tuple : list of filenames
                        for fil, v in d.items():
                            for m in v:
                                inverted_replica_dict[m].append(fil)

                        replica_rereplication = inverted_replica_dict[node] # filenames in failed node
                        print("Files to be replicated: ", replica_rereplication)

                        threads = []
                        results = [0] * len(replica_rereplication)
                        for i, filename in enumerate(replica_rereplication):
                            t = threading.Thread(target=self.rereplication_leader_helper, args=(filename, node, results, i))
                            threads.append(t)
                        
                        for t in threads:
                            t.start()
                        for t in threads: 
                            t.join()

                        if sum(results) == len(replica_rereplication):
                            print(f"Re-replication done: {datetime.datetime.now()}")
                            # print("Popping", self.machine.membership_list.failed_nodes[0])
                            self.machine.membership_list.failed_nodes.pop(0)
                            print("\n")

                        # Check if any maple juice task was running
                        print(node, self.machine.membership_list.task_running_workers)
                        if node in self.machine.membership_list.task_running_workers:
                            print(f"Maple Juice Task was Running in {node}")
                            task = self.machine.membership_list.task_running_workers[node]
                            self.task_queue.append(task)
                            self.machine.membership_list.task_running_workers.pop(node)



    def write_replicas(self, filename, replica):
        # print("Sending message to", replica)
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((replica[0], replica[3] + BASE_WRITE_PORT)) 

        # Send filename message to the replicas
        self.send_message(sock_fd, pickle.dumps(filename))
        print("receiving data from replica {}", replica)
        data = sock_fd.recv(MAX)
        print("received data from replica {}", replica)

        # If file is opened, send the file content
        if "ACK" == pickle.loads(data):

            with open(os.path.join(HOME_DIR, filename), 'rb') as f:
                bytes_read = f.read()
                # if not bytes_read:
                    # break

                while bytes_read:
                    self.send_message(sock_fd, bytes_read)
                    bytes_read = f.read()
            
            sock_fd.shutdown(socket.SHUT_WR)
            data = sock_fd.recv(MAX)
            mssg = pickle.loads(data)
            sock_fd.close()

            if mssg.type == "ACK":
                return 1

        return 0


    def rereplication_follower(self):
        ''' Receive the message from leader to re-replicate the files stored in the failed node '''
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.rereplication_port))
        sock_fd.listen(5)

        while True:
            if self.machine.status == "Joined"  and self.leader_node is not None:
                # if self.machine.nodeId[3] != self.leader_node[3]:
                conn, addr = sock_fd.accept()
                try:
                    data = conn.recv(MAX)   # receive serialized data from another machine
                    mssg = pickle.loads(data)

                    if mssg.type == 'put_replica':
                        filename = mssg.kwargs['filename']
                        replica_node = mssg.kwargs['replica_node']
                        print(f"[Re-Replication] Putting Replica of {filename} in {replica_node}")
                        ret = self.write_replicas(filename, replica_node)
                        self.machine.logger.info(f"[Re-Replication] Putting Replica of {filename} in {replica_node}")
                        if ret == 1:
                            print("Wrote replica successfully")
                            mssg = Message(msg_type='ACK',
                                            host=self.machine.nodeId,
                                            membership_list=None,
                                            counter=None
                                            )
                            self.send_message(conn, pickle.dumps(mssg))
                        else:
                            print("Wrote replica unsuccessfully")
                            mssg = Message(msg_type='NACK',
                                            host=self.machine.nodeId,
                                            membership_list=None,
                                            counter=None
                                            )
                            self.send_message(conn, pickle.dumps(mssg))
                finally:
                    conn.close()



    def read_replicas(self, sdfs_filename, local_filename, replicas):
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
        # print("[ACK Received] Get file successfully\n")


    def receive_writes(self):        
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.write_port))
        sock_fd.listen(5)

        while True:
            conn, addr = sock_fd.accept()
            try:
                # Receive Filename and open the file. If file exists, ACK
                data = conn.recv(MAX)
                mode = pickle.loads(data)

                data = conn.recv(MAX)
                self.machine.logger.info(f"[Write] Filename received: {pickle.loads(data)}")
                print(f"[Write] Filename received: {pickle.loads(data)}")
                filename = pickle.loads(data)

                f = open(os.path.join(HOME_DIR, filename), mode)
                if f:
                    conn.sendall(pickle.dumps("ACK"))

                # Receive file content, write to the file and send ACK
                self.machine.logger.info("[Write] Receiving file content...")
                
                bytes_read = conn.recv(self.machine.BUFFER_SIZE)
                while bytes_read:
                    if not bytes_read:
                        break
                    else:
                        # write to the file the bytes we just received
                        f.write(bytes_read)
                        bytes_read = conn.recv(self.machine.BUFFER_SIZE)

                f.close()

                # Sending ACK to leader
                self.filestore_ping('w', filename)
                
                # print(f"[Write ACK] Writing Done at VM{self.machine.nodeId[3]}\n")
                mssg = Message(msg_type='ACK',
                                host=self.machine.nodeId,
                                membership_list=None,
                                counter=None,
                              )
                self.send_message(conn, pickle.dumps(mssg))

            finally:
                conn.close()


    def receive_reads(self):
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.read_port))
        sock_fd.listen(5)

        while True:
            conn, addr = sock_fd.accept()
            try:
                # Receive Filename and open the file. If file exists, ACK
                data = conn.recv(MAX)
                self.machine.logger.info(f"[Read] Filename received: {pickle.loads(data)}")
                filename = pickle.loads(data)

                with open(os.path.join(HOME_DIR, filename), 'rb') as f:
                    bytes_read = f.read()
                    if not bytes_read:
                        break

                    while bytes_read:
                        self.send_message(conn, bytes_read)
                        bytes_read = f.read()
                
                conn.shutdown(socket.SHUT_WR)
                self.filestore_ping('r', filename)
                
            finally:
                conn.close()
        

    def receive_deletes(self):
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.delete_port))
        sock_fd.listen(5)

        while True:
            conn, addr = sock_fd.accept()
            try:
                # Receive Filename and open the file. If file exists, ACK
                data = conn.recv(MAX)
                self.machine.logger.info(f"[Delete] Filename received: {pickle.loads(data)}")
                filename = pickle.loads(data)

                os.remove(os.path.join(HOME_DIR, filename))

                self.filestore_ping('d', filename)
                mssg = Message(msg_type='ACK',
                                host=self.machine.nodeId,
                                membership_list=None,
                                counter=None,
                              )
                self.send_message(conn, pickle.dumps(mssg))
                
            finally:
                conn.close()


    def perform_maple(self, mssg):
        filename = mssg.kwargs['filename']
        filename_replica = mssg.kwargs['file_replica']
        maple_exe = mssg.kwargs['maple_exe']
        exe_replica = mssg.kwargs['exe_replica']
        condition = mssg.kwargs['condition']
        print(f"SDFS Prefix: {mssg.kwargs['sdfs_prefix']}")

        # Read the exe file and the input data to the local machine
        if not os.path.exists(f'{HOME_DIR}/tmp'):
            os.mkdir(f'{HOME_DIR}/tmp')
        local_filepath = f'{HOME_DIR}/tmp/' + filename
        self.read_replicas(filename, local_filepath, filename_replica)

        local_exe_file = f'{HOME_DIR}/tmp/' + maple_exe
        self.read_replicas(maple_exe, local_exe_file, exe_replica)

        # Execute the exe file with the input data
        os.system(f"chmod +x {local_exe_file}")
        if condition == None:
            os.system(f"{local_exe_file} {local_filepath} > {HOME_DIR}/tmp/output.txt")
        else:
            os.system(f"{local_exe_file} {local_filepath} \"{condition}\" > {HOME_DIR}/tmp/output.txt")

        # Read the output written by the exe file
        with open(f'{HOME_DIR}/tmp/output.txt', 'r') as f:
            print("Processing files...")
            lines = f.readlines()
            for line in lines:
                print(line.split(',', 1))
                K = line.split(',', 1)[0] # row id is the key
                V = line.split(',', 1)[1]
                if K == '':
                    continue
                print(K,V)

                # Append the output to the correct file in the SDFS
                local_tmp_file = 'tmp.txt'
                sdfs_intermediate_filename = mssg.kwargs['sdfs_prefix'] + f"_{K}"
                print("SDFS Intermediate Filename: ", sdfs_intermediate_filename)
                with open(local_tmp_file, 'w') as f:
                    f.write(V)
                
                with open(local_tmp_file, 'r') as f:
                    print(f.readlines())

                leader_node = self.get_leader_node()
                handler.handle_put(self.machine, leader_node, self.ip, 'ab', local_tmp_file, sdfs_intermediate_filename)
                os.remove(local_tmp_file)

        print("Processed all files.")
        time.sleep(2)
        # Sending ACK to leader
        self.filestore_ping('m', filename)



    def perform_juice(self, mssg):
        print("Performing juice")
        filename = mssg.kwargs['filename']
        filename_replica = mssg.kwargs['file_replica']
        juice_exe = mssg.kwargs['juice_exe']
        exe_replica = mssg.kwargs['exe_replica']

        # Read the exe file and the input data to the local machine
        if not os.path.exists(f'{HOME_DIR}/tmp'):
            os.mkdir(f'{HOME_DIR}/tmp')
        local_filepath = f'{HOME_DIR}/tmp/' + filename
        self.read_replicas(filename, local_filepath, filename_replica)

        local_exe_file = f'{HOME_DIR}/tmp/' + juice_exe
        self.read_replicas(juice_exe, local_exe_file, exe_replica)

        with open(local_filepath, 'r') as f:
            lines = f.readlines()
            print(lines)

        os.system(f"chmod +x {local_exe_file}")
        os.system(f"{local_exe_file} {local_filepath} > {HOME_DIR}/tmp/output.txt")

        # Read the output written by the exe file
        local_output_file = f'{HOME_DIR}/tmp/output.txt'
        leader_node = self.get_leader_node()
        with open(local_output_file, 'r') as f:
            lines = f.readlines()
            print(lines)
        handler.handle_put(self.machine, leader_node, self.ip, 'ab', local_output_file, mssg.kwargs['dest_filename'])
        os.remove(local_output_file)
        
        time.sleep(2)
        # Sending ACK to leader
        self.filestore_ping('j', filename)


    def receive_task(self):
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.bind((self.ip, self.maplejuice_port))
        print(self.ip, self.maplejuice_port)
        sock_fd.listen(5)

        while True:
            conn, addr = sock_fd.accept()
            try:
                # Receive the maple message
                data = conn.recv(MAX)
                mssg = pickle.loads(data)

                if mssg.type == 'maple':
                    self.perform_maple(mssg)
                
                elif mssg.type == 'juice':
                    self.perform_juice(mssg)

            finally:
                try:
                    shutil.rmtree(f'{HOME_DIR}/tmp')
                except:
                    pass
                conn.close()


    def add_row_id_column(self, input_files):
        '''
            Add a row_id column to the input files
        '''
        line_ctr = 1
        for filename in input_files:
            replicas = self.leader_work('get', filename)
            local_filename = f'{HOME_DIR}/tmp/' + filename
            self.read_replicas(filename, local_filename, replicas)
            output = []
            with open(local_filename, 'r') as f:
                lines = f.readlines()
                schema_line = lines[0]
                schema_line = "row_id," + schema_line
                output.append(schema_line)
                
                for i, line in enumerate(lines[1:]):
                    line = str(line_ctr) + "," + line
                    output.append(line)
                    line_ctr += 1
                
            with open(f'{HOME_DIR}/tmp/{filename}', 'w') as f_out:
                for line in output:
                    f_out.write(line)
    
    
    def partition_input_data(self, input_files, num_maples, demo1=False):
        '''
          Paritition the list of input files into num_maples number of files
        '''
        total_lines = 0
        if not os.path.exists(f'{HOME_DIR}/tmp'):
            os.mkdir(f'{HOME_DIR}/tmp')
        
        if not demo1:
            self.add_row_id_column(input_files)
        else:
            for filename in input_files:
                replicas = self.leader_work('get', filename)
                local_filename = f'{HOME_DIR}/tmp/' + filename
                self.read_replicas(filename, local_filename, replicas)

        schema_line = None
        for filename in input_files:
            # replicas = self.leader_work('get', filename)
            local_filename = f'{HOME_DIR}/tmp/' + filename
            # self.read_replicas(filename, local_filename, replicas)

            with open(local_filename, 'rb') as f:
                lines = f.readlines()
                if not demo1:
                    schema_line = lines[0]
                    num_lines = len(lines) - 1
                else:
                    schema_line = b''
                    num_lines = len(lines)
                
                total_lines += num_lines

        num_maples = min(num_maples, total_lines)
        lines_per_file = total_lines // num_maples

        written_files = []
        if not demo1:
            to_be_writen_files = {filename: 1 for filename in input_files}
        else:
            to_be_writen_files = {filename: 0 for filename in input_files}
        new_maple_files = []

        # For each maple task, we create a new file and store it in SDFS
        for i in range(num_maples):
            n_lines_written = 0
            n_lines_to_write = lines_per_file
            tmp_file = f"tmp_{i}.txt"

            # Open the file in append mode
            with open(f"{HOME_DIR}/tmp/{tmp_file}", 'ab') as f_out:
                # Write the schema line to the file
                f_out.write(schema_line)

                # For each input file that we get, we write the lines_per_file number of lines to f_out
                # If the number of lines in the input file is less than lines_per_file, we write all the lines
                # If the number of lines in the input file is more than lines_per_file, we write only the first lines_per_file lines
                for filename in input_files:
                    if filename not in written_files:
                        with open(f"{HOME_DIR}/tmp/{filename}", 'rb') as f_in:
                            lines = f_in.readlines()

                            if filename in to_be_writen_files:
                                start_index = to_be_writen_files[filename]
                                # If it is the last file, write all the remaining lines
                                # Else write only the lines_per_file number of lines
                                if i == num_maples - 1:
                                    end_index = len(lines)
                                else:
                                    end_index = start_index + n_lines_to_write

                                if end_index <= len(lines):
                                    for line in lines[start_index : end_index]:
                                        f_out.write(line)
                                    # Increase the starting index of the input file for next write    
                                    to_be_writen_files[filename] += n_lines_to_write
                                    break
                                else:
                                    for line in lines:
                                        f_out.write(line)
                                    # Indicate that the input file has been completely written
                                    written_files.append(filename)
                                    to_be_writen_files.pop(filename)
                                    # Reduce the number of lines to write for the current file
                                    n_lines_to_write -= len(lines) - start_index
            
            # Put the file in SDFS
            new_maple_files.append(tmp_file)
            replicas = self.leader_work('put', tmp_file)
            handler.handle_put(self.machine, self.leader_node, self.ip, 'wb', f"{HOME_DIR}/tmp/{tmp_file}", tmp_file, replicas)

        shutil.rmtree(f'{HOME_DIR}/tmp')
        return new_maple_files



    def shuffle(self, sdfs_prefix, num_juices):
        all_filenames = list(self.machine.membership_list.file_replication_dict.keys())
        juice_filenames = [filename for filename in all_filenames if filename.startswith(sdfs_prefix)]

        return juice_filenames



    def dequeue_task(self):
        while True:
            if len(self.task_queue) > 0:
                active_nodes = list(self.machine.membership_list.active_nodes.keys())
                task_running_nodes = list(self.machine.membership_list.task_running_workers.keys())
                available_nodes = list(set(active_nodes) - set(task_running_nodes))
                task_type = self.task_queue[0][0]

                if len(available_nodes) > 0:

                    if task_type == 'maple':
                        
                        task_type, filename, exe, sdfs_prefix, condition = self.task_queue.pop(0)
                        try:
                            worker_id = hash(filename) % len(available_nodes)
                            worker = available_nodes[worker_id]

                            exe_replica = self.leader_work('get', exe)
                            file_replica = self.leader_work('get', filename)

                            sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            sock_fd.connect((worker[0], worker[3] + BASE_MAPLEJUICE_PORT))

                            mssg = Message(msg_type='maple',
                                            host=self.machine.nodeId,
                                            membership_list=None,
                                            counter=None,
                                            maple_exe=exe,
                                            exe_replica=exe_replica,
                                            sdfs_prefix=sdfs_prefix,
                                            filename=filename,
                                            file_replica=file_replica,
                                            condition=condition,
                                            )
                            # self.machine.membership_list.task_running_workers[worker] = (task_type, filename, exe, sdfs_prefix, condition)
                            self.send_message(sock_fd, pickle.dumps(mssg))
                            sock_fd.close()
                        except:
                            # time.sleep(T_MAPLEJUICE_FAIL_WAIT)
                            self.task_queue.append(('maple', filename, exe, sdfs_prefix, condition))  # can uncomment
                            print("Maple task failed. Added to task queue again")
                            pass

                    elif task_type == 'juice':

                        task_type, filename, exe, dest_filename = self.task_queue.pop(0)
                        try:
                            worker_id = hash(filename) % len(available_nodes) # Hash partitioning
                            worker = available_nodes[worker_id]

                            exe_replica = self.leader_work('get', exe)
                            file_replica = self.leader_work('get', filename)

                            sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            sock_fd.connect((worker[0], worker[3] + BASE_MAPLEJUICE_PORT))

                            mssg = Message(msg_type='juice',
                                            host=self.machine.nodeId,
                                            membership_list=None,
                                            counter=None,
                                            juice_exe=exe,
                                            exe_replica=exe_replica,
                                            filename=filename,
                                            file_replica=file_replica,
                                            dest_filename=dest_filename
                                            )
                            # self.machine.membership_list.task_running_workers[worker] = (task_type, filename, exe, sdfs_prefix, condition)
                            self.send_message(sock_fd, pickle.dumps(mssg))
                            sock_fd.close()
                        except:
                            # time.sleep(T_MAPLEJUICE_FAIL_WAIT)
                            self.task_queue.append(('juice', filename, exe, dest_filename)) # can uncomment
                            print("Juice task failed. Added to task queue again")
                            pass



    def execute_maple_phase(self, mssg):
        maple_exe = mssg.kwargs['maple_exe']
        num_maples = int(mssg.kwargs['num_maples'])
        sdfs_prefix = mssg.kwargs['sdfs_prefix']
        input_files = eval(mssg.kwargs['input_files'])
        query = mssg.kwargs['query']
        try:
            demo1 = mssg.kwargs['demo1']
        except:
            demo1 = False

        self.maple_input_files = input_files

        new_input_files = self.partition_input_data(input_files, num_maples, demo1)
        print("File Partition Done")
        self.machine.logger.info("File Partition Done")

        # Find the where condition from query
        if query:
            condition = query.split('where')[1].strip()
        else:
            condition = None

        for filename in new_input_files:
            self.maple_tasks_completed += 1
            self.task_queue.append(('maple', filename, maple_exe, sdfs_prefix, condition))
        
        # Wait till all the maple tasks are completed
        while self.maple_tasks_completed:
            continue

        time.sleep(2)


    def execute_juice_phase(self, mssg):
        juice_exe = mssg.kwargs['juice_exe']
        num_juices = int(mssg.kwargs['num_juices'])
        sdfs_prefix = mssg.kwargs['sdfs_prefix']
        dest_filename = mssg.kwargs['dest_filename']
        delete_intermediate = int(mssg.kwargs['delete_intermediate'])
        try:
            demo1 = mssg.kwargs['demo1']
        except:
            demo1 = False

        new_input_files = self.shuffle(sdfs_prefix, num_juices)
        print(new_input_files)
        print("Shuffling Done")
        self.machine.logger.info("Shuffling Done")

        for filename in new_input_files:
            self.juice_tasks_completed += 1
            self.task_queue.append(('juice', filename, juice_exe, dest_filename))

        # Wait till all the juice tasks are completed
        while self.juice_tasks_completed:
            continue

        if delete_intermediate:
            # Delete the intermediate files
            for filename in new_input_files:
                replicas = self.leader_work('delete', filename)
                handler.handle_delete(self.machine, self.leader_node, filename, replicas)
                print(f"Deleted {filename}")

        time.sleep(2)

        
        if not demo1:
            # Get the output file and the keys of the maple-juice task
            local_output_file = f'{HOME_DIR}/{dest_filename}'
            replicas = self.leader_work('get', dest_filename)
            print(replicas)
            self.read_replicas(dest_filename, local_output_file, replicas)
            print("destination file read")

            output_lines = None
            with open(local_output_file, 'r') as f:
                output_lines = f.readlines()

            output_lines = [output.split('\n')[0] for output in output_lines]
            line_ctr = 1
            for filename in self.maple_input_files:
                with open(filename, 'r') as f:
                    lines = f.readlines()
                    schema_line = lines[0]
                    
                    for i, line in enumerate(lines[1:]):
                        if str(line_ctr) in output_lines:
                            index = output_lines.index(str(line_ctr))
                            output_lines[index] = line
                        line_ctr += 1
            
            with open(local_output_file, 'w') as f:
                for line in output_lines:
                    f.write(line)

            replicas = self.leader_work('put', dest_filename)
            handler.handle_put(self.machine, self.leader_node, self.ip, 'wb', local_output_file, dest_filename, replicas)
        


    def execute_maplejuice(self, mssg):
        self.execute_maple_phase(mssg)
        self.execute_juice_phase(mssg)



    def demo1(self, mssg):
        # Map Reduce 1
        new_mssg = Message(msg_type="maple",
                            host=mssg.host,
                            membership_list=None,
                            counter=None,
                            maple_exe=mssg.kwargs['maple1_exe'],
                            num_maples=mssg.kwargs['num_maples'],
                            sdfs_prefix="initialprefix",
                            input_files=mssg.kwargs['input_files'],
                            query=mssg.kwargs['query'],
                            )
        self.execute_maple_phase(new_mssg)

        new_mssg = Message(msg_type="juice",
                            host=mssg.host,
                            membership_list=None,
                            counter=None,
                            juice_exe=mssg.kwargs['juice1_exe'],
                            num_juices=mssg.kwargs['num_juices'],
                            sdfs_prefix="initialprefix",
                            dest_filename="intermediate.txt",
                            delete_intermediate="0",
                            demo1=True,
                            )
        self.execute_juice_phase(new_mssg)

        # Map Reduce 2
        new_mssg = Message(msg_type="maple",
                            host=mssg.host,
                            membership_list=None,
                            counter=None,
                            maple_exe=mssg.kwargs['maple2_exe'],
                            num_maples=mssg.kwargs['num_maples'],
                            sdfs_prefix=mssg.kwargs['sdfs_prefix'],
                            input_files="['intermediate.txt']",
                            query=None,
                            demo1=True,
                            )
        self.execute_maple_phase(new_mssg)

        new_mssg = Message(msg_type="juice",
                            host=mssg.host,
                            membership_list=None,
                            counter=None,
                            juice_exe=mssg.kwargs['juice2_exe'],
                            num_juices=mssg.kwargs['num_juices'],
                            sdfs_prefix=mssg.kwargs['sdfs_prefix'],
                            dest_filename=mssg.kwargs['dest_filename'],
                            delete_intermediate="0",
                            demo1=True,
                            )
        self.execute_juice_phase(new_mssg)



    def leader_work(self, mssg_type, sdfs_filename):
        ''' Leader should process messages about reads/writes/deletes '''

        if mssg_type == 'put':
            # For Write messages       
            while True:
                # if (self.machine.membership_list.read_lock_dict[sdfs_filename] == 0):
                if (sdfs_filename not in self.machine.membership_list.write_lock_set) and \
                    (self.machine.membership_list.read_lock_dict[sdfs_filename] == 0):
                    self.machine.membership_list.write_lock_set.add(sdfs_filename)
                    self.machine.logger.info(f"[Write] File Lock Set: {self.machine.membership_list.write_lock_set}")

                    if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                        # Choose Replication Servers if file is present
                        servers = self.machine.membership_list.active_nodes.keys()
                        replica_servers = random.sample(servers, self.machine.REPLICATION_FACTOR)
                        replica_servers = [(server[0], server[1] - BASE_PORT + BASE_WRITE_PORT, server[2], server[3]) for server in replica_servers]
                    else:
                        replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
                        replica_servers = [(server[0], server[1] - BASE_PORT + BASE_WRITE_PORT, server[2], server[3]) for server in replica_servers]

                    break           

        elif mssg_type == 'get':
            # For Read messages
            while True:
                if sdfs_filename not in self.machine.membership_list.write_lock_set:
                    self.machine.membership_list.read_lock_dict[sdfs_filename] += 1
                    if self.machine.membership_list.read_lock_dict[sdfs_filename] <= 50:
                        
                        # self.machine.membership_list.read_lock_dict[sdfs_filename] += 1
                        if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                            replica_servers = []
                        else:
                            # Use the replication servers from the membership list
                            replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
                            replica_servers = [(server[0], server[1] - BASE_PORT + BASE_READ_PORT, server[2], server[3]) for server in replica_servers]
                        break
        
        elif mssg_type == 'multiread':
                        
            if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                replica_servers = []
            else:
                # Use the replication servers from the membership list
                replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
                replica_servers = [(server[0], server[1] - BASE_PORT + BASE_READ_PORT, server[2], server[3]) for server in replica_servers]
                            
        elif mssg_type == 'delete':

            # For Delete messages
            if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                replica_servers = []
            else:
                replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
                replica_servers = [(server[0], server[1] - BASE_PORT + BASE_DELETE_PORT, server[2], server[3]) for server in replica_servers]

        elif mssg_type == 'ls':
            # For ls messages
            if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                replica_servers = []
            else:
                replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
        
        return replica_servers



    def implement_write(self, recv_mssg):
        time.sleep(1)
        replicas = self.leader_work(recv_mssg.type, recv_mssg.kwargs['filename'])
        mssg = Message(msg_type='replica',
                        host=self.machine.nodeId,
                        membership_list=None,
                        counter=None,
                        replica=replicas
                        )

        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((recv_mssg.host[0], recv_mssg.kwargs["port"]))

        self.send_message(sock_fd, pickle.dumps(mssg))
        self.machine.logger.info("Message regarding replicas sent to client")
        sock_fd.close()



    def implement_read(self, recv_mssg):
        time.sleep(1)
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((recv_mssg.host[0], recv_mssg.kwargs["port"]))

        replicas = self.leader_work(recv_mssg.type, recv_mssg.kwargs['filename'])
        if len(replicas) == 0:
            mssg = Message(msg_type='NACK',
                            host=self.machine.nodeId,
                            membership_list=None,
                            counter=None,
                            replica=None
                            )
            self.send_message(sock_fd, pickle.dumps(mssg))
        else:
            mssg = Message(msg_type='replica',
                            host=self.machine.nodeId,
                            membership_list=None,
                            counter=None,
                            replica=replicas
                            )
            self.send_message(sock_fd, pickle.dumps(mssg))
        sock_fd.close()



    def dequeue_read_write(self):
        while True:
            if len(self.write_queue) > 0 and len(self.read_queue) == 0:
                self.op_timestamps.pop(0)
                write_req = self.write_queue.pop(0)
                recv_mssg = write_req[1]
                self.implement_write(recv_mssg)

            elif len(self.write_queue) == 0 and len(self.read_queue) > 0:
                self.op_timestamps.pop(0)
                read_req = self.read_queue.pop(0)
                recv_mssg = read_req[1]
                self.implement_read(recv_mssg)

            elif len(self.write_queue) > 0 and len(self.read_queue) > 0:
                if self.read_queue[0][2] >= 4:
                    while True:
                        if len(self.read_queue) > 0  and self.read_queue[0][2] >= 4:
                            read_req = self.read_queue.pop(0)
                            self.op_timestamps.remove(read_req[0])
                            for i in range(len(self.write_queue)):
                                self.write_queue[i] = (self.write_queue[i][0], self.write_queue[i][1], self.write_queue[i][2]+1)
                            self.implement_read(read_req[1])
                        else:
                            break

                elif self.write_queue[0][2] >= 4:
                    while True:
                        if len(self.write_queue) > 0 and self.write_queue[0][2] >= 4:
                            write_req = self.write_queue.pop(0)
                            self.op_timestamps.remove(write_req[0])
                            for i in range(len(self.read_queue)):
                                self.read_queue[i] = (self.read_queue[i][0], self.read_queue[i][1], self.read_queue[i][2]+1)
                            self.implement_write(write_req[1])
                        else:
                            break

                else:
                    op = self.op_timestamps.pop(0)
                    if 'w' in op:
                        write_req = self.write_queue.pop(0)
                        for i in range(len(self.read_queue)):
                            self.read_queue[i] = (self.read_queue[i][0], self.read_queue[i][1], self.read_queue[i][2]+1)
                        self.implement_write(write_req[1])
                    else:
                        read_req = self.read_queue.pop(0)
                        for i in range(len(self.write_queue)):
                            self.write_queue[i] = (self.write_queue[i][0], self.write_queue[i][1], self.write_queue[i][2]+1)
                        self.implement_read(read_req[1])

            else:
                continue



    def receive(self):
        ''' Receive messages and act accordingly (get/put/delete) '''
        recv_sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        recv_sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        recv_sock_fd.bind((self.ip, self.port))
        recv_sock_fd.listen(5)

        while True:
            conn, addr = recv_sock_fd.accept()

            try:
                data = conn.recv(MAX) # receive serialized data from another machine
                recv_mssg = pickle.loads(data)

                if recv_mssg.type == 'multiread_get':
                    if os.fork() == 0:
                        self.read_replicas(recv_mssg.kwargs['sdfs_filename'], recv_mssg.kwargs['local_filename'], recv_mssg.kwargs['replica'])
                        

                if self.machine.nodeId[3] != self.leader_node[3]:
                    # If not leader, ask client to send to leader
                    mssg = Message(msg_type='leader',
                                    host=self.machine.nodeId,
                                    membership_list=None,
                                    counter=None,
                                    leader=self.leader_node
                                    )
                    self.send_message(conn, pickle.dumps(mssg))  

                else:
                    if recv_mssg.type == 'put':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            self.machine.logger.info("[Receive at Leader] Put message received at leader")
                            hash = random.getrandbits(128)
                            self.write_queue.append((f"{hash}w", recv_mssg, 0))
                            self.op_timestamps.append(f"{hash}w")
                            '''
                            replicas = self.leader_work(recv_mssg.type, recv_mssg.kwargs['filename'])
                            mssg = Message(msg_type='replica',
                                            host=self.machine.nodeId,
                                            membership_list=None,
                                            counter=None,
                                            replica=replicas
                                            )
                            self.send_message(conn, pickle.dumps(mssg))
                            self.machine.logger.info("Message regarding replicas sent to client")
                            '''

                    elif recv_mssg.type == 'get':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            hash = random.getrandbits(128)
                            self.read_queue.append((f"{hash}r", recv_mssg, 0))
                            self.op_timestamps.append(f"{hash}r")
                            '''
                            replicas = self.leader_work(recv_mssg.type, recv_mssg.kwargs['filename'])
                            if len(replicas) == 0:
                                mssg = Message(msg_type='NACK',
                                                host=self.machine.nodeId,
                                                membership_list=None,
                                                counter=None,
                                                replica=None
                                                )
                                self.send_message(conn, pickle.dumps(mssg))
                            else:
                                mssg = Message(msg_type='replica',
                                                host=self.machine.nodeId,
                                                membership_list=None,
                                                counter=None,
                                                replica=replicas
                                                )
                                self.send_message(conn, pickle.dumps(mssg))
                            '''

                    elif recv_mssg.type == 'multiread':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            replicas = self.leader_work(recv_mssg.type, recv_mssg.kwargs['sdfs_filename'])
                            if len(replicas) == 0:
                                mssg = Message(msg_type='NACK',
                                                host=self.machine.nodeId,
                                                membership_list=None,
                                                counter=None,
                                                replica=None
                                                )
                                self.send_message(conn, pickle.dumps(mssg))
                            else:
                                # For each VM in the multiread group, send the replica list
                                sdfs_filename = recv_mssg.kwargs['sdfs_filename']

                                for machine in recv_mssg.kwargs['machines']:
                                    while True:
                                        if sdfs_filename not in self.machine.membership_list.write_lock_set:
                                            if self.machine.membership_list.read_lock_dict[sdfs_filename] < 2:
                                                self.machine.membership_list.read_lock_dict[sdfs_filename] += 1

                                                machine_num = int(machine[2:])
                                                
                                                if machine_num == recv_mssg.host[3]:
                                                    mssg = Message(msg_type='replica',
                                                                    host=self.machine.nodeId,
                                                                    membership_list=None,
                                                                    counter=None,
                                                                    replica=replicas
                                                                    )
                                                    self.send_message(conn, pickle.dumps(mssg))
                                                else:
                                                    all_nodes = list(self.machine.membership_list.active_nodes.keys())
                                                    # Find the VM IP and Port where multiread need to initiated
                                                    send_node = None
                                                    for node in all_nodes:
                                                        if node[3] == machine_num:
                                                            send_node = node
                                                            break
                                                    
                                                    new_sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                                    new_sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                                                    new_sock_fd.connect((send_node[0], BASE_FS_PORT  + send_node[3]))

                                                    mssg = Message(msg_type='multiread_get',
                                                                    host=self.machine.nodeId,
                                                                    membership_list=None,
                                                                    counter=None,
                                                                    sdfs_filename=recv_mssg.kwargs['sdfs_filename'],
                                                                    local_filename=recv_mssg.kwargs['local_filename'],
                                                                    replica=replicas
                                                                    )
                                                    self.send_message(new_sock_fd, pickle.dumps(mssg))
                                                    new_sock_fd.close()
                                                
                                                break
                                        else:
                                            time.sleep(3)


                    elif recv_mssg.type == 'delete':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            replicas = self.leader_work(recv_mssg.type, recv_mssg.kwargs['filename'])
                            if len(replicas) == 0:
                                mssg = Message(msg_type='NACK',
                                                host=self.machine.nodeId,
                                                membership_list=None,
                                                counter=None,
                                                replica=None
                                                )
                                self.send_message(conn, pickle.dumps(mssg))
                            else:
                                mssg = Message(msg_type='replica',
                                                host=self.machine.nodeId,
                                                membership_list=None,
                                                counter=None,
                                                replica=replicas
                                                )
                                self.send_message(conn, pickle.dumps(mssg))

                    elif recv_mssg.type == 'ls':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            replicas = self.leader_work(recv_mssg.type, recv_mssg.kwargs['sdfsfilename'])
                            if len(replicas) == 0:
                                mssg = Message(msg_type='NACK',
                                                host=self.machine.nodeId,
                                                membership_list=None,
                                                counter=None,
                                                replica=None
                                                )
                                self.send_message(conn, pickle.dumps(mssg))
                            else:
                                mssg = Message(msg_type='replica',
                                                host=self.machine.nodeId,
                                                membership_list=None,
                                                counter=None,
                                                replica=replicas
                                                )
                                self.send_message(conn, pickle.dumps(mssg))

                    elif recv_mssg.type == 'maple':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            thread_m = threading.Thread(target=self.execute_maple_phase, args=(recv_mssg, ))
                            thread_m.start()

                    elif recv_mssg.type == 'juice':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            thread_j = threading.Thread(target=self.execute_juice_phase, args=(recv_mssg, ))
                            thread_j.start()
                    
                    elif recv_mssg.type == 'maplejuice':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            thread_mj = threading.Thread(target=self.execute_maplejuice, args=(recv_mssg, ))
                            thread_mj.start()

                    elif recv_mssg.type == 'demo1':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            thread_d1 = threading.Thread(target=self.demo1, args=(recv_mssg, ))
                            thread_d1.start()

            finally:
                conn.close()


    def start_machine(self):
        ''' Start the server '''
        leader_election_thread = threading.Thread(target=self.leader_election)
        receive_thread = threading.Thread(target=self.receive)
        dequeue_thread = threading.Thread(target=self.dequeue_read_write)
        receive_writes_thread = threading.Thread(target=self.receive_writes)
        receive_reads_thread = threading.Thread(target=self.receive_reads)
        receive_deletes_thread = threading.Thread(target=self.receive_deletes)
        filestore_ping_recv_thread = threading.Thread(target=self.filestore_ping_recv)
        rereplication_leader_thread = threading.Thread(target=self.rereplication_leader)
        rereplication_follower_thread = threading.Thread(target=self.rereplication_follower)

        dequeue_task_thread = threading.Thread(target=self.dequeue_task)
        receive_task_thread = threading.Thread(target=self.receive_task)

        leader_election_thread.start()
        receive_thread.start()
        dequeue_thread.start()
        receive_writes_thread.start()
        receive_reads_thread.start()
        receive_deletes_thread.start()
        filestore_ping_recv_thread.start()
        rereplication_leader_thread.start()
        rereplication_follower_thread.start()

        dequeue_task_thread.start()
        receive_task_thread.start()
    


# if __name__ == "__main__":
#     MACHINE_NUM = sys.argv[1]

#     machine = Machine(int(MACHINE_NUM))
#     machine.start_machine()