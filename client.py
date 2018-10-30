import json
import os
import socket
import struct
import time

from utils import log


def rpc(sock, func, params):
    message = json.dumps({'func': func, 'params': params}).encode('utf-8')
    length_prefix = struct.pack('I', len(message))
    sock.sendall(length_prefix)
    sock.sendall(message)
    log(sock, sock.fileno(), 'sended')
    length_prefix = sock.recv(4)
    length, = struct.unpack('I', length_prefix)
    resp = json.loads(sock.recv(length).decode('utf-8'))
    return resp['status'], resp['result']


if __name__ == '__main__':
    for _ in range(10):
        if os.fork() == 0:
            break
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 8086))
    for i in range(10):
        status, result = rpc(s, 'pi', i)
        print('status: {}'.format(status))
        print(result)
        time.sleep(1)
    s.close()
