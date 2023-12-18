import os

def project_sync(hosts):
    for host in hosts:
        os.system("rsync -av --exclude='.git/' --exclude='data/' --exclude='models/' --exclude='updates/' -e 'ssh -o StrictHostKeyChecking=no' --delete /root/FL_Architecture/ root@" + host + ":/root/FL_Architecture")


def setup(rsync_hosts):
    for host in rsync_hosts:
        # os.system("rsync /Users/sc134/OneDrive - University of Illinois - Urbana/Distributed_Systems/cs-425-mp2 sc134@" + host + ":/home/sc134/")
        # os.system("rsync -r /home/sc134/cs-425-mp3 sc134@" + host + ":/home/sc134/")
        # os.system("rsync -r /home/raunaks3/cs-425-mp3 raunaks3@" + host + ":/home/raunaks3/")
        # os.system("rsync -r /home/raunaks3/.ssh/ hadoopuser@" + host + ":~/.ssh/")
        # os.system("rsync -r /home/raunaks3/.bashrc  raunaks3@" + host + ":/home/raunaks3/")
        os.system("rsync -ar ~/hadoop/ hadoopuser@" + host + ":~/hadoop")
        # os.system("rsync -ar ~/hadoop/etc/ hadoopuser@" + host + ":~/hadoop/etc/")



    # run(pssh_hosts, "python3 main.py &")
    # os.system("python3 main.py")


rsync_hosts = [
        # "fa23-cs425-3701.cs.illinois.edu",
        # "fa23-cs425-3702.cs.illinois.edu",
        # "fa23-cs425-3703.cs.illinois.edu",
        # "fa23-cs425-3704.cs.illinois.edu",
        # "fa23-cs425-3705.cs.illinois.edu",
        # "fa23-cs425-3706.cs.illinois.edu",
        "fa23-cs425-3707.cs.illinois.edu",
        "fa23-cs425-3708.cs.illinois.edu",
        "fa23-cs425-3709.cs.illinois.edu",
        "fa23-cs425-3710.cs.illinois.edu",    
        ]

setup(rsync_hosts)
