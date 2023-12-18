from client import Client
import socket

if __name__ == "__main__":
    
    host = socket.gethostname()
    print("HOST: ", host)
    machine_num = int(host.split('.')[0].split('-')[2][2:])

    machine = Client(machine_num)
    machine.start_machine()