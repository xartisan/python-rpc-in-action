import json
import struct
import sys
import time


def make_response(result, status=666):
    resp = {'status': status, 'result': result}
    resp_bytes = json.dumps(resp).encode('utf-8')
    length_prefix = len(resp_bytes)
    length_prefix_bytes = struct.pack('I', length_prefix)
    return length_prefix_bytes + resp_bytes


def bad_response(result=None):
    return make_response(result, 0)


def pretty_time(t=None, fmt='%H:%M:%S'):
    t = t or time.localtime(int(time.time()))
    dt = time.strftime(fmt, t)
    return dt


def log(*args, **kwargs):
    print(pretty_time(), *args, **kwargs)


def log_error(*args, **kwargs):
    print(pretty_time(), *args, **kwargs, file=sys.stderr)
