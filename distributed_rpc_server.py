import asyncore
import errno
import json
import math
import os
import signal
import socket
import struct
import sys
from io import BytesIO

from cowpy import cow
from kazoo.client import KazooClient

from utils import make_response, bad_response, log, log_error


class RPCHandler(asyncore.dispatcher_with_send):

    def __init__(self, sock, addr):
        super().__init__(sock=sock)
        self.addr = addr
        self.handlers = {
            'foo': self.foo,
            'pi': self.pi
        }
        self.rbuf = BytesIO()

    def handle_connect(self):
        log(self.addr, 'comes')

    def handle_close(self):
        log(self.addr, 'bye')
        self.close()

    def handle_read(self):
        while True:
            chunk = self.recv(1024)
            if chunk:
                self.rbuf.write(chunk)
            if len(chunk) < 1024:
                break
        self.handle_rpc()

    def handle_rpc(self):
        self.rbuf.seek(0)
        pos = 0
        while True:
            length_prefix_bytes = self.rbuf.read(4)
            if len(length_prefix_bytes) < 4:
                break
            length_prefix, = struct.unpack('I', length_prefix_bytes)
            req_bytes = self.rbuf.read(length_prefix)
            if len(req_bytes) < length_prefix:
                break
            req = json.loads(req_bytes)
            try:
                func, params = req['func'], req['params']
                func = self.handlers[func]
                rv = func(params)
                self.send(make_response(rv))
            except KeyError as e:
                log(e)
                self.send(bad_response())
            pos = self.rbuf.tell()
        lefts = self.rbuf.getvalue()[pos:]
        self.rbuf = BytesIO()
        self.rbuf.write(lefts)

    def foo(self, params):
        return cow.milk_random_cow(params)

    def pi(self, n):
        n = int(n)
        s = 0.0
        for i in range(n + 1):
            s += 1.0 / (2 * i + 1) / (2 * i + 1)
        return math.sqrt(8 * s)


class RPCServer(asyncore.dispatcher):
    zk_root = '/demo'
    zk_rpc = os.path.join(zk_root, 'rpc')

    def __init__(self, host, port):
        super().__init__()
        self.host = host
        self.port = port
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(1)
        self.child_ps = []
        log(f'master {os.getpid()} started')
        if self.prefork(5):
            self.register_zk()
            self.register_parent_signal()
        else:
            self.register_child_signal()

    def prefork(self, n):
        for i in range(n):
            pid = os.fork()
            if pid < 0:
                raise Exception('fork error')
            elif pid == 0:
                log(f'slave {os.getpid()} created!')
                return False
            else:
                self.child_ps.append(pid)
        return True

    def register_zk(self):
        log(f'master {os.getpid()} register zookeeper')
        self.zk = KazooClient(hosts='127.0.0.1:2181')
        self.zk.start()
        self.zk.ensure_path(self.zk_root)
        value = json.dumps({'host': self.host, 'port': self.port}).encode('utf-8')
        self.zk.create(self.zk_rpc, value, ephemeral=True, sequence=True)

    def parent_exit_handler(self, sig, frame):
        self.zk.stop()
        self.close()
        asyncore.close_all()

        pids = []
        for pid in self.child_ps:
            log(f'killing pid {pid}')
            try:
                os.kill(pid, signal.SIGINT)
                pids.append(pid)
            except OSError as e:
                if e.args[0] != errno.ECHILD:
                    raise

            log(f'killed {pid}')

        log('total killed', '\t'.join(str(pid) for pid in pids) + ' killed')
        for pid in pids:
            while True:
                try:
                    os.waitpid(pid, 0)
                    break
                except OSError as e:
                    if e.args[0] == errno.ECHILD:
                        break
                    if e.args != errno.EINTR:
                        raise e
            log(f'waiting completes {pid}')

    def reap_child(self, sig, frame):
        log('before reaping')
        while True:
            try:
                info = os.waitpid(-1, os.WNOHANG)
                break
            except OSError as e:
                if e.args[0] == errno.ECHILD:
                    log_error('no child process to reap')
                    return
                if e.args[0] != errno.EINTR:
                    raise e
        pid = info[0]
        try:
            self.child_ps.remove(pid)
        except ValueError:
            pass
        log(f'after reaping {pid}')

    def exit_child(self, sig, frame):
        self.close()
        asyncore.close_all()
        log('all closed')

    def register_parent_signal(self):
        signal.signal(signal.SIGINT, self.parent_exit_handler)
        signal.signal(signal.SIGTERM, self.parent_exit_handler)
        signal.signal(signal.SIGCHLD, self.reap_child)

    def register_child_signal(self):
        signal.signal(signal.SIGINT, self.exit_child)
        signal.signal(signal.SIGTERM, self.exit_child)

    def handle_accept(self):
        t = self.accept()
        if t is not None:
            sock, addr = t
            RPCHandler(sock, addr)


if __name__ == '__main__':
    host = sys.argv[1]
    port = int(sys.argv[2])
    RPCServer(host, port)
    asyncore.loop()
