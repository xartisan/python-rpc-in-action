import json
import socket
import struct
from threading import Thread

from cowpy import cow

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


def make_response(result, status=666):
    resp = {'status': status, 'result': result}
    resp_bytes = json.dumps(resp).encode('utf-8')
    length_prefix = len(resp_bytes)
    length_prefix_bytes = struct.pack('I', length_prefix)
    return length_prefix_bytes + resp_bytes


def bad_response(result=None):
    return make_response(result, 0)


def handle(conn: socket.socket):
    while True:
        length_prefix = conn.recv(4)
        if not length_prefix:
            conn.close()
            break
        length, = struct.unpack('I', length_prefix)
        req = json.loads(conn.recv(length).decode('utf-8'))
        func_name, params = req['func'], req['params']
        try:
            result = rpc_funcs[func_name](params)
        except IndexError as e:
            print(e)
            conn.sendall(bad_response())
        else:
            conn.sendall(make_response(result))


def serve(sock: socket.socket):
    while True:
        conn, addr = sock.accept()
        print('*** New Connections from {}'.format(*addr))
        Thread(target=handle, args=(conn,)).start()


def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('localhost', 8080))
    s.listen(1)
    serve(s)


if __name__ == '__main__':
    main()
