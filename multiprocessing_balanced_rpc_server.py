import json
import os
import socket
import struct

from cowpy import cow

from utils import make_response, bad_response

rpc_funcs = {}


def register_rpc_func(func):
    rpc_funcs[func.__name__] = func
    return func


@register_rpc_func
def foo(params):
    return cow.milk_random_cow(params)


@register_rpc_func
def dummy(params):
    return params


def handle_conn(conn: socket.socket, addr):
    print(os.getpid(), f'handling connection from: {conn} {addr}')
    while True:
        length_prefix_bytes = conn.recv(4)
        if not length_prefix_bytes:
            print(addr, 'bye')
            conn.close()
            break
        length_prefix, = struct.unpack('I', length_prefix_bytes)
        req_bytes = conn.recv(length_prefix)
        req = json.loads(req_bytes)
        try:
            func, params = req['func'], req['params']
            func = rpc_funcs[func]
            rv = func(params)
            conn.sendall(make_response(rv))
        except KeyError as e:
            print(e)
            conn.sendall(bad_response())


def slave_server(pr: socket.socket):
    while True:
        bufsize = 1
        ancsize = socket.CMSG_LEN(struct.calcsize('i'))
        msg, ancdata, flags, addr = pr.recvmsg(bufsize, ancsize)
        cmsg_level, cmsg_type, cmsg_data = ancdata[0]
        fd = struct.unpack('i', cmsg_data)[0]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, fileno=fd)
        handle_conn(sock, sock.getpeername())


def master_server(server_sock: socket.socket, pws):
    idx = 0
    while True:
        conn, addr = server_sock.accept()
        print(os.getpid(), 'master accepts', conn)
        pw = pws[idx % len(pws)]
        msg = [b'x']
        ancdata = [(
            socket.SOL_SOCKET,
            socket.SCM_RIGHTS,
            struct.pack('i', conn.fileno())
        )]
        pw.sendmsg(msg, ancdata)
        # conn.close()
        idx += 1


def pre_fork(server_sock, n):
    pws = []
    for i in range(n):
        pr, pw = socket.socketpair()
        pid = os.fork()
        if pid < 0:
            return pws
        elif pid == 0:
            print(os.getpid(), 'slave created')
            pw.close()
            server_sock.close()
            return pr
        else:
            pws.append(pw)
            pr.close()
    return pws


def main():
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(('localhost', 8080))
    server_sock.listen(1)
    print(os.getpid(), 'server socket', server_sock, server_sock.fileno())
    pws_or_pr = pre_fork(server_sock, 3)
    if isinstance(pws_or_pr, list):
        if not pws_or_pr:
            return
        master_server(server_sock, pws_or_pr)
        print(pws_or_pr)
    else:
        slave_server(pws_or_pr)


if __name__ == '__main__':
    main()
