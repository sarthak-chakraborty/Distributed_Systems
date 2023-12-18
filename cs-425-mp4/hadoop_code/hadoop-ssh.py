# from pssh.agent import SSHAgent
# from pssh.utils import load_private_key
# from pssh.clients.native.parallel import ParallelSSHClient
# from gevent import joinall
# from pssh.utils import enable_host_logger
import os
# enable_host_logger()


# def run(hosts, command):
#     client = ParallelSSHClient(hosts)
#     output = client.run_command(command)
#     client.join(output, consume_output=True)
#     """for host, host_output in output.items():
#         print(host, " : ")
#         for line in host_output.stdout:
#             print(line)
#         for line in host_output.stderr:
#             print(line)"""



def project_sync(hosts):
    for host in hosts:
        os.system("rsync -av --exclude='.git/' --exclude='data/' --exclude='models/' --exclude='updates/' -e 'ssh -o StrictHostKeyChecking=no' --delete /root/FL_Architecture/ root@" + host + ":/root/FL_Architecture")


def setup(rsync_hosts, pssh_hosts):
    for host in rsync_hosts:
        # os.system("rsync /Users/sc134/OneDrive - University of Illinois - Urbana/Distributed_Systems/cs-425-mp2 sc134@" + host + ":/home/sc134/")
        # os.system("rsync -r /home/sc134/cs-425-mp4 sc134@" + host + ":/home/sc134/")
        os.system("rsync -a /home/raunaks3/hadoop raunaks3@" + host + ":/home/raunaks3/")
    
    os.system("rsync -a /home/raunaks3/.bashrc raunaks3@" + host + ":/home/raunaks3/")


    # run(pssh_hosts, "python3 main.py &")

    # os.system("python3 main.py")


rsync_hosts = [
        # "fa23-cs425-3701.cs.illinois.edu",
         "fa23-cs425-3702.cs.illinois.edu",
        #  "fa23-cs425-3703.cs.illinois.edu",
        #  "fa23-cs425-3704.cs.illinois.edu",
        #  "fa23-cs425-3705.cs.illinois.edu",
        #  "fa23-cs425-3706.cs.illinois.edu",
        #  "fa23-cs425-3707.cs.illinois.edu",
        #  "fa23-cs425-3708.cs.illinois.edu",
        #  "fa23-cs425-3709.cs.illinois.edu",
        #  "fa23-cs425-3710.cs.illinois.edu",    
        ]

pssh_hosts = [
        # "sc134@fa23-cs425-3701.cs.illinois.edu",
         "sc134@fa23-cs425-3702.cs.illinois.edu",
         "sc134@fa23-cs425-3703.cs.illinois.edu",
         "fa23-cs425-3704.cs.illinois.edu",
         "fa23-cs425-3705.cs.illinois.edu",
         "fa23-cs425-3706.cs.illinois.edu",
         "fa23-cs425-3707.cs.illinois.edu",
         "fa23-cs425-3708.cs.illinois.edu",
         "fa23-cs425-3709.cs.illinois.edu",
         "fa23-cs425-3710.cs.illinois.edu",    
        ]

# pssh_hosts = [
#         # "sc134@fa23-cs425-3701.cs.illinois.edu",
#          "raunaks3@fa23-cs425-3702.cs.illinois.edu",
#          "raunaks3@fa23-cs425-3703.cs.illinois.edu",
#          "raunaks3@fa23-cs425-3704.cs.illinois.edu",
#          "raunaks3@fa23-cs425-3705.cs.illinois.edu",
#          "raunaks3@fa23-cs425-3706.cs.illinois.edu",
#          "raunaks3@fa23-cs425-3707.cs.illinois.edu",
#          "raunaks3@fa23-cs425-3708.cs.illinois.edu",
#          "raunaks3@fa23-cs425-3709.cs.illinois.edu",
#         #  "raunaks3@fa23-cs425-3710.cs.illinois.edu",    
#         ]


setup(rsync_hosts, pssh_hosts)