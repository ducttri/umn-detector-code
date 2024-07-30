import ctypes
import struct
import gzip
import sys
import shutil
import json
import numpy as np
import umndet.common.impress_exact_structs as ies
from typing import Any, Callable, IO, Iterable

def generic_read_binary(
    fn: str,
    open_func: Callable,
    function_body: Callable[[IO[bytes]], Any]
) -> list[Any]: 
    ret = []
    with gzip.GzipFile(fileobj=fn, mode='rb') as f:
        while True:
            try:
                new_data = function_body(f)
            except struct.error:
                break
            if not new_data: break
            ret.append(new_data)
    return ret

def read_binary(fn: str, type_: type, open_func: Callable) -> list:
    sz = ctypes.sizeof(type_)
    def read_elt(f: IO[bytes]):
        d = type_()
        eof = (f.readinto(d) != sz)
        if eof: return None
        return d

    return generic_read_binary(fn, open_func, read_elt)

def read_det_health(fn: str, open_func: Callable) -> list[ies.DetectorHealth]:
    return read_binary(fn, ies.DetectorHealth, open_func)

def collapse_json(data: list[dict[str, object]]):
    collapse_keys = tuple(data[0].keys())
    ret = dict()

    for datum in data:
        for k in collapse_keys:
            try:
                ret[k]['value'].append(datum[k]['value'])
            except KeyError:
                ret[k] = {
                    # Only assign unit once here, not above in the `try`
                    'unit': datum[k]['unit'],
                    'value': [datum[k]['value']]
                }

    return ret

def collapse_health(dat: list[dict[str, object]]) -> list[dict[str, object]]:
    detectors = ('c1', 'm1', 'm5', 'x1', 'x123')
    ret = dict()

    for detector in detectors:
        ret[detector] = collapse_json([d[detector] for d in dat])

    ret |= {
        'timestamp': [d['timestamp'] for d in dat]
    }
    return ret



def main():
    health_data = []
    health_data += read_det_health(sys.stdin.buffer, gzip.GzipFile)

    # with gzip.GzipFile(fileobj=sys.stdin.buffer, mode='rb') as f_in:
    #     shutil.copyfileobj(f_in, sys.stdout.buffer)

    


    jsonified = [hd.to_json() for hd in health_data]
    jsonified.sort(key=lambda e: e['timestamp'])
    collapsed = collapse_health(jsonified)

    processed_data = {}
    processed_data['start_time'] = collapsed['timestamp'][0]
    
    for i in ['c1', 'm1', 'm5', 'x1']:
        processed_data[i] = {}
        for j in ['arm_temp', 'sipm_temp', 'sipm_operating_voltage']:
            processed_data[i][j] = (cur_proc := {})
            cur_data = collapsed[i][j]['value']

            cur_proc['avg'] = np.mean(cur_data)
            cur_proc['min'] = min(cur_data)
            cur_proc['max'] = max(cur_data)

    processed_data['x123'] = (x123_proc := {})
    for j in ['board_temp', 'det_high_voltage', 'det_temp']:
        x123_proc[j] = (cur_proc := {})
        cur_data = collapsed['x123'][j]['value']

        cur_proc['avg'] = np.mean(cur_data)
        cur_proc['min'] = min(cur_data)
        cur_proc['max'] = max(cur_data)

    final_data = {}
    final_data['processed_data'] = processed_data
    final_data['raw_data'] = collapsed

    json_str = json.dumps(final_data)
    sys.stdout.buffer.write(json_str.encode('utf-8'))
    

if __name__ == '__main__':
    main()
