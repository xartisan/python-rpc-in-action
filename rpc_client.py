import json
import random
import socket
import struct
import time

from kazoo.client import KazooClient

from utils import log

zk_root = '/demo'

G = {'servers': []}


class RemoteServer:

    def __init__(self, addr):
        self.addr = addr
        self._socket = None

    @property
    def socket(self):
        if not self._socket:
            self.connect()
        return self._socket

    def rpc(self, func, params):
        sock = self.socket
        message = json.dumps({'func': func, 'params': params}).encode('utf-8')
        length_prefix = struct.pack('I', len(message))
        sock.sendall(length_prefix)
        sock.sendall(message)
        log(sock, sock.fileno(), 'sended')
        length_prefix = sock.recv(4)
        length, = struct.unpack('I', length_prefix)
        resp = json.loads(sock.recv(length).decode('utf-8'))
        return resp['status'], resp['result']

    def connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host, port = self.addr.split(':')
        sock.connect((host, int(port)))
        self._socket = sock

    def reconnect(self):
        self.close()
        self.connect()

    def close(self):
        if self._socket:
            self._socket.close()
            self._socket = None

    def __getattr__(self, func_name):
        def _(params):
            return self.rpc(func_name, params)

        setattr(self, func_name, _)
        return _


def get_servers():
    zk = KazooClient(hosts='127.0.0.1:2181')
    zk.start()
    current_addrs = set()

    def watch_servers(_):
        new_addrs = set()

        for child in zk.get_children(zk_root, watch=watch_servers):
            node = zk.get(zk_root + '/' + child)
            addr = json.loads(node[0])
            new_addrs.add('%s:%d' % (addr['host'], addr['port']))
        added_addrs = new_addrs - current_addrs
        removed_addrs = current_addrs - new_addrs
        removed_servers = []
        for server in G['servers']:
            if server.addr == removed_addrs:
                removed_servers.append(server)

        for server in removed_servers:
            G['servers'].remove(server)
            current_addrs.remove(server.addr)
            log(f'Remove server {server.addr}')

        for addr in added_addrs:
            current_addrs.add(addr)
            G['servers'].append(RemoteServer(addr))
            log(f'Add server {server.addr}')

    for child in zk.get_children(zk_root, watch=watch_servers):
        node = zk.get(zk_root + '/' + child)
        addr = json.loads(node[0])
        log(addr)
        current_addrs.add('%s:%d' % (addr['host'], addr['port']))
    G['servers'] = [RemoteServer(s) for s in current_addrs]
    return G['servers']


def random_server() -> RemoteServer:
    if not G['servers']:
        get_servers()
    if not G['servers']:
        return

    return random.choice(G['servers'])


if __name__ == '__main__':
    for i in range(100):
        server = random_server()
        if not server:
            break
        time.sleep(0.5)
        try:
            status, result = server.foo('request %d' % i)
            print(server.addr, status, result)
        except Exception as e:
            server.close()
            print(e)
        server = random_server()
        if not server:
            break
        time.sleep(0.5)
        try:
            status, result = server.pi(i)
            print(server.addr, status, result)
        except Exception as e:
            server.close()
            print(e)
