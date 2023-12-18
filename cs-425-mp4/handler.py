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

RAND_PORT = 57757
MAX = 8192

def send_message(sock_fd, msg):
    ''' Send a message to another machine '''
    try:
        sock_fd.sendall(msg)
    except:
        pass


def write_replicas(mode, replica, local_filename, sdfs_filename, ack_count, index):
    ''' Write a file to the replica '''
    sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # sock_fd.connect((replica[0], replica[1]))
    try:
        sock_fd.connect((replica[0], replica[1]))
        # Send mode to the replicas
        send_message(sock_fd, pickle.dumps(mode))

        time.sleep(0.2)
        # Send filename to the replicas
        send_message(sock_fd, pickle.dumps(sdfs_filename))
        data = sock_fd.recv(MAX)
        # If file is opened, send the file content
        if "ACK" == pickle.loads(data):

            with open(local_filename, 'rb') as f:
                bytes_read = f.read()
                if not bytes_read:
                    return

                while bytes_read:
                    send_message(sock_fd, bytes_read)
                    bytes_read = f.read()
            
            sock_fd.shutdown(socket.SHUT_WR)
            data = sock_fd.recv(MAX)
            mssg = pickle.loads(data)
            sock_fd.close()

            # If acknowledgment received, increment the ack_count
            if mssg.type == "ACK":
                ack_count[index] = 1
    except:
        print("Error in write_replicas of handler.")
        return


def handle_put(machine_obj, leader_node, ip, mode, local_filename, sdfs_filename, replicas=None):
    ''' Put a file in the SDFS '''
    put_start_time = datetime.datetime.now()

    if replicas is None:
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((leader_node[0], leader_node[1]))

        put_mssg = Message(msg_type="put", 
                        host=machine_obj.nodeId,
                        membership_list=None,
                        counter=None,
                        filename=sdfs_filename,
                        port=RAND_PORT
                        )                      
        # Send put message to the leader
        send_message(sock_fd, pickle.dumps(put_mssg))
        machine_obj.logger.info(f'Put Message sent to Leader Node')
        sock_fd.close()

        # Open a socket to get the replicas where the file is stored
        recv_sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        recv_sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        recv_sock_fd.bind((ip, RAND_PORT))
        recv_sock_fd.listen(1)

        conn, addr = recv_sock_fd.accept()
        data = conn.recv(MAX)
        mssg = pickle.loads(data)
        conn.close()

        if mssg.type == "replica":
            replicas = mssg.kwargs['replica']
        else:
            print("Error: Replica message not received")
            return

    machine_obj.logger.info(f"Replica Servers: {replicas}")

    # Send the file to all the replicas parallely
    threads = []
    ack_count = [0] * len(replicas)
    for i, replica in enumerate(replicas):
        t = threading.Thread(target=write_replicas, args=(mode, replica, local_filename, sdfs_filename, ack_count, i))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    if sum(ack_count) >= machine_obj.WRITE_QUORUM:
        put_end_time = datetime.datetime.now()
        print("[ACK Received] Put file successfully")
        # print(f"Replicas where {local_filename} is stored: VM{replicas[0][3]}, VM{replicas[1][3]}, VM{replicas[2][3]}, VM{replicas[3][3]}")
        print(f"Total Time Taken: {(put_end_time - put_start_time).total_seconds()}\n")

    else:
        print("[ACK Not Received] Put file unsuccessfully\n")



def delete_replicas(machine_obj, sdfs_filename, replica_servers):
    ack_count = 0
    for replica in replica_servers:
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((replica[0], replica[1])) 

        # Send filename message to the replicas
        send_message(sock_fd, pickle.dumps(sdfs_filename))
        data = sock_fd.recv(MAX)
        msg = pickle.loads(data)

        if msg.type == "ACK":
            ack_count += 1

        sock_fd.close()
    
    if ack_count == machine_obj.REPLICATION_FACTOR:
        print("[ACK Received] Deleted file successfully\n")
    else:
        print("[ACK Not Received] Deleted file unsuccessfully\n")
    

def handle_delete(machine_obj, leader_node, sdfs_filename, replicas=None):
    ''' Delete a file from the SDFS '''

    if replicas is None:
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((leader_node[0], leader_node[1])) 

        delete_mssg = Message(msg_type="delete", 
                        host=machine_obj.nodeId,
                        membership_list=None,
                        counter=None,
                        filename=sdfs_filename,
                        )                      

        send_message(sock_fd, pickle.dumps(delete_mssg))
        data = sock_fd.recv(MAX)
        mssg = pickle.loads(data)
        sock_fd.close()

        if mssg.type == "replica":
            machine_obj.logger.info(f"Replica Servers: {mssg.kwargs['replica']}")
            replicas = mssg.kwargs['replica']
            delete_replicas(machine_obj, sdfs_filename, replicas)
        elif mssg.type == "NACK":
            print("[ACK not Received] File not found in SDFS\n")

    else:
        delete_replicas(machine_obj, sdfs_filename, replicas)

