import json
import socket
import struct
import time


def rpc(sock, func, params):
    message = json.dumps({'func': func, 'params': params}).encode('utf-8')
    length_prefix = struct.pack('I', len(message))
    sock.sendall(length_prefix)
    sock.sendall(message)
    length_prefix = sock.recv(4)
    length, = struct.unpack('I', length_prefix)
    resp = json.loads(sock.recv(length).decode('utf-8'))
    return resp['status'], resp['result']


if __name__ == '__main__':
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 8080))
    for i in range(10):
        status, result = rpc(s, 'foo', 'request {}'.format(i))
        print('status: {}'.format(status))
        print(result)
        time.sleep(1)
    s.close()
