import sys
import logging
import os.path
import MySQLdb
import argparse
import ConfigParser
import multiprocessing
import subprocess
from SocketServer import StreamRequestHandler, ThreadingTCPServer


class Slave(object):

    def __init__(self, id, user, passwd, address, port):
        self.id = int(id)
        self.user = str(user)
        self.passwd = str(passwd)
        self.address = str(address)
        self.port = int(port)

    def get_position(self):
        con = MySQLdb.connect(host=self.address,
                              user=self.user,
                              passwd=self.passwd,
                              port=self.port)
        with con:
            cursor = con.cursor()
            cursor.execute('show slave status')
            row = cursor.fetchall()
            pos = row[0][21]
            Logger.debug('Slave position: {0}'.format(pos)) 
            return int(pos)


class MysqlSlaveService():

    def __init__(self):
        self.user = None
        self.passwd = None
        self.slaves = []

    @staticmethod
    def _parse_arguments(*args):
        p = argparse.ArgumentParser()
        p.add_argument('--defaults-file',
                       dest='config_file',
                       help='mysql-proxy configuration')
        p.add_argument('--proxy-read-only-backend-addresses', '-r',
                       dest='proxy_ro_address',
                       help='address:port of the remote slave-server (default: not set)')
        return p.parse_args(args)

    def gen_config(self, *arg):
        conf = self._parse_arguments(*arg)
        if conf.config_file:
            c = ConfigParser.ConfigParser()
            c.readfp(open(conf.config_file, 'r'))
            if c.has_option('mysql-proxy', 'proxy-read-only-backend-addresses'):
                slave_addresses = [tuple(item.split(':')) for item in
                                   c.get('mysql-proxy', 'proxy-read-only-backend-addresses').split(',') if item]
                Logger.debug('Get slave addresses: {0}, user: {1}, passwd: {2}'.format(slave_addresses, self.user, self.passwd))
                self.slaves = [Slave(slave_addresses.index((addr, port)) + 2,
                                     self.user,
                                     self.passwd,
                                     addr, port) for addr, port in slave_addresses]

        else:
            if conf.proxy_ro_address:
                slave_addresses = [tuple(item.split(':')) for item in conf.proxy_ro_address.split(',') if item]
                Logger.debug('Get slave addresses: {0}'.format(slave_addresses))
                self.slaves = [Slave(slave_addresses.index((addr, port)) + 2,
                                     self.user,
                                     self.passwd,
                                     addr, port) for addr, port in slave_addresses]

MysqlSlave = MysqlSlaveService()

class MysqlSlaveHandler(StreamRequestHandler):

    def update_slave(self, m_pos):
        for slave in MysqlSlave.slaves:
            s_pos = slave.get_position()
            Logger.debug("Slave id:{0}, get position:{1}".format(slave.id, s_pos))
            if s_pos >= m_pos:
                return '{0}:{1}'.format(slave.id, s_pos)

    def handle(self):
        while True:
            try:
                data = self.rfile.readline().strip()
                if data:
                    Logger.debug("Get data:{0}".format(data))
                    result = self.update_slave(int(data))
                    self.wfile.write(result)
            except Exception, err:
                Logger.error(err)


class Proxy(multiprocessing.Process):
    def __init__(self, server):
        multiprocessing.Process.__init__(self)
        self.server = server

    def run(self):
        self.server.serve_forever()


def print_help():
    print('Usage:'
          '  mysql-proxy.py CONFIG_FILE [Mysql-Proxy OPTION]')
    exit(1)

def parse_log_level(string):
    tb = {
        'info': logging.INFO,
        'debug': logging.DEBUG,
        'warn': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }
    if string in tb:
        return tb[string]

EXEC = 'mysql-proxy'
if len(sys.argv) < 2:
    print_help()
CONFIG = sys.argv[1]
try:
    os.path.exists(CONFIG)
except:
    print_help()

ARGS = sys.argv[2:]

C = ConfigParser.ConfigParser()
C.readfp(open(CONFIG, 'r'))
SECT = 'DEFAULT'

try:
    ADDRESS = C.get(SECT, 'slave_server_address')
except:
    ADDRESS = '127.0.0.1'

try:
    PORT = int(C.get(SECT, 'slave_server_port'))
except:
    PORT = 8081

try:
    LOG_PATH = C.get(SECT, 'slave_server_log_path')
except:
    LOG_PATH = '/tmp/slave_server.log'

try:
    LOG_LEVEL = parse_log_level(C.get(SECT, 'slave_server_log_level'))
except:
    LOG_LEVEL = logging.CRITICAL

try:
    USER = C.get(SECT, 'user')
except:
    raise Exception('Lost user name of backend servers.')

try:
    PASSWD = C.get(SECT, 'passwd')
except:
    raise Exception('Lost password of backend servers.')

logging.basicConfig(level=LOG_LEVEL,
                    format='[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)s] %(message)s',
                    filename=LOG_PATH)
Logger = logging.getLogger(__name__)

if __name__ == '__main__':
    MysqlSlave.user = USER
    MysqlSlave.passwd = PASSWD
    MysqlSlave.gen_config(*ARGS)
    address = (ADDRESS, PORT)
    Logger.info("Slave Server is starting on {0}:{1}".format(ADDRESS, PORT))
    server = Proxy(ThreadingTCPServer(address, MysqlSlaveHandler))
    server.start()
    LUA_PATH = os.path.dirname(os.path.realpath(__file__)) + '/?.lua'
    Logger.info('Set LUA path: {0}'.format(LUA_PATH))
    os.environ["LUA_PATH"] = LUA_PATH
    cmd = EXEC + ' ' + ' '.join(ARGS) + ' '
    Logger.info('Mysql-Proxy is starting, execute cmd:{0} ...'.format(cmd))
    subprocess.call(cmd, shell=True, env=os.environ) 
