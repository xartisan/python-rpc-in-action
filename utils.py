import json
import struct


def make_response(result, status=666):
    resp = {'status': status, 'result': result}
    resp_bytes = json.dumps(resp).encode('utf-8')
    length_prefix = len(resp_bytes)
    length_prefix_bytes = struct.pack('I', length_prefix)
    return length_prefix_bytes + resp_bytes


def bad_response(result=None):
    return make_response(result, 0)
