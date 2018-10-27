import asyncore
import json
import os
import socket
import struct
from io import BytesIO

from cowpy import cow

from utils import make_response, bad_response


class RPCHandler(asyncore.dispatcher_with_send):

    def __init__(self, sock, addr):
        super().__init__(sock=sock)
        self.addr = addr
        self.handlers = {'foo': self.foo}
        self.rbuf = BytesIO()

    @staticmethod
    def foo(params):
        return cow.milk_random_cow(str(params))

    def handle_connect(self):
        print(self.addr, 'comes')

    def handle_close(self):
        print(self.addr, 'bye')
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
        while True:
            length_prefix_bytes = self.rbuf.read(4)
            if len(length_prefix_bytes) < 4:
                break
            length_prefix, = struct.unpack('I', length_prefix_bytes)
            req_bytes = self.rbuf.read(length_prefix)
            if len(req_bytes) < length_prefix:
                break
            req = json.loads(req_bytes.decode('utf-8'))
            try:
                func, params = req['func'], req['params']
                func = self.handlers[func]
                rv = func(params)
                return self.send(make_response(rv))
            except KeyError as e:
                print(e)
                self.send(bad_response())
            lefts = self.rbuf.getvalue()[:length_prefix + 4]
            self.rbuf = BytesIO()
            self.rbuf.write(lefts)

        self.rbuf.seek(0)


class RPCServer(asyncore.dispatcher):

    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(1)
        self.fork(5)

    def fork(self, n):
        for _ in range(n):
            pid = os.fork()
            if pid < 0:
                break
            elif pid == 0:
                break

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock, addr = pair
            RPCHandler(sock, addr)


if __name__ == '__main__':
    RPCServer("localhost", 8080)
    asyncore.loop()
