import ctypes
import struct
import gzip
import sys
import shutil
import json
import numpy as np
import umndet.common.impress_exact_structs as ies
from typing import Any, Callable, IO, Iterable

hafxHealthField = {
    'arm_temp':	'ARM processor temperature',
    'sipm_temp':	'SiPM board temperature',
    'sipm_operating_voltage':	'SiPM operating voltage',
    'sipm_target_voltage':	'SiPM target voltage',
    'counts':	'Counts',
    'dead_time':	'Dead time',
    'real_time':	'Real time',
}

x123HealthField = {
    'board_temp':	'DP5 board temperature',
    'det_high_voltage':	'Detector high voltage',
    'det_temp':	'Detector head temperature',
    'fast_counts':	'Fast shaper # of counts',
    'slow_counts':	'Slow shaper # of counts',
    'accumulation_time':	'Accumulation Time',
    'real_time':	'Real time'
}

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
        data = [d[detector] for d in dat]
        collapse_keys = tuple(data[0].keys())

        for datum in data:
            for k in collapse_keys:
                try:
                    ret[detector+"_"+k]['value'].append(datum[k]['value'])
                except KeyError:
                    ret[detector+"_"+k] = {
                        # Only assign unit once here, not above in the `try`
                        'unit': datum[k]['unit'],
                        'value': [datum[k]['value']]
                    }

    ret |= {
        'timestamp': [d['timestamp'] for d in dat]
    }
    return ret



def main():
    health_data = []
    health_data += read_det_health(sys.stdin.buffer, gzip.GzipFile)

    jsonified = [hd.to_json() for hd in health_data]
    jsonified.sort(key=lambda e: e['timestamp'])
    collapsed = collapse_health(jsonified)

    processed_data = {}
    raw_data = []
    processed_data['start_time'] = collapsed['timestamp'][0]
    
    for i in ['c1', 'm1', 'm5', 'x1']:
        processed_data[i] = {}
        for j in ['arm_temp', 'sipm_temp', 'sipm_operating_voltage']:
            processed_data[i][j] = (cur_proc := {})
            cur_data = collapsed[i+"_"+j]['value']

            cur_proc['avg'] = round(np.mean(cur_data), 2)
            cur_proc['min'] = round(min(cur_data), 2)
            cur_proc['max'] = round(max(cur_data), 2)

        for j in tuple(hafxHealthField.keys()):
            raw_data.append({
                'type' : i,
                'field' : hafxHealthField[j],
                'unit' : collapsed[i+"_"+j]['unit'],
                'value' : collapsed[i+"_"+j]['value']
            })
            
            

    processed_data['x123'] = (x123_proc := {})
    for j in ['board_temp', 'det_high_voltage', 'det_temp']:
        x123_proc[j] = (cur_proc := {})
        cur_data = collapsed['x123_'+j]['value']

        cur_proc['avg'] = round(np.mean(cur_data), 2)
        cur_proc['min'] = round(min(cur_data), 2)
        cur_proc['max'] = round(max(cur_data), 2)

    for j in tuple(x123HealthField.keys()):
        raw_data.append({
            'type' : 'x123',
            'field' : x123HealthField[j],
            'unit' : collapsed["x123_"+j]['unit'],
            'value' : collapsed["x123_"+j]['value']
        })

    raw_data.append({
        'type' : 'general',
        'field' : 'Time stamp',
        'unit' : '',
        'value': collapsed['timestamp']
    })

    final_data = {}
    final_data['processed_data'] = processed_data
    final_data['raw_data'] = raw_data

    json_str = json.dumps(final_data)
    sys.stdout.buffer.write(json_str.encode('utf-8'))
    

if __name__ == '__main__':
    main()


    
    # for detector in detectors:
    #     collapse_keys = tuple(data[0].keys())
    #     for k in collapse_keys:
    #         raw_data.append({
    #             'type' : detector,
    #             'field' : healthField[k],
    #             'unit' : ret[detector+"_"+k]['unit'],
    #             'value' : ret[detector+"_"+k]['value']
    #         })

    # raw_data.append({
    #     'type' : 'timestamp',
    #     'field' : 'UTC Time',
    #     'unit' : ret[detector+"_"+k]['unit'],
    #     'value' : ret[detector+"_"+k]['value']
    # })

    # collapse['original'] = ret
    # collapse['raw_data'] = raw_data