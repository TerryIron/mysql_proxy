import sys
import logging
import socket


class MysqlSlaveClient(object):
    def __init__(self, ip_address, port, master_pos):
        self.address = (ip_address, int(port))
        self.master_pos = master_pos

    def run(self):
        s = socket.socket()
        s.connect(self.address)
        s.sendall(str(self.master_pos)+'\n')
        data = s.recv(1024)
        if data:
	    print(data)


def print_help():
    print('Usage:'
          '  client.py ADDRESS PORT MASTER_POS')
    exit(1)


def main():
    if len(sys.argv) < 3:
        print_help()
    address = sys.argv[1]
    port = sys.argv[2]
    master_pos = sys.argv[3]
    if master_pos and address and port:
        c = MysqlSlaveClient(address, port, master_pos)
        c.run()

if __name__ == '__main__':
    main()
