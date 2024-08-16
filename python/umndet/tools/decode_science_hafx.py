import argparse
import datetime as dt
import json
import gzip
import struct
import numpy as np
import sys
import ctypes
from typing import Any, Callable, IO, Iterable

import umndet.common.impress_exact_structs as ies
import umndet.common.constants as umncon

def get_proper_timedelta(file_name):
    '''
    Rebinned science data will have different time deltas between events.
    This is because if we sum along the time axis, the counts in the
    spectrogram can be considered to be bounded by wider time edges.

    Make this a function so that we can update it if we change the rebinning scheme down the line.
    '''
    # File name format is: IDENT_DATE_#.extension
    identifier, date_str, _ = file_name.split('_')
    date = dt.datetime.strptime(date_str, umncon.DATE_FMT)

    slice_width = dt.timedelta(seconds=1 / 32) 

    if 'time' in identifier:
        # Add more revisions as appropriate
        if date >= umncon.FIRST_REVISION:
            return umncon.FIRST_NUM_TIMES_REBIN * slice_width

    return slice_width

def get_data_format(fn: str) -> str:
    '''
    Depending on the file naming convention used by the rebinner,
    we can either be dealing with:
        - "raw" aka full-resolution data
        - rebinned across time
        - rebinned across energy
        - rebinneda cross time and energy
    '''
    possibilities = ('time+energy', 'time', 'energy')
    for p in possibilities:
        if fn.startswith(p): return p

    # No rebinning has happened; return something useful
    return 'full_resolution'

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

def read_hafx_sci(fn: str, open_func: Callable) -> list[ies.NominalHafx]:
    return read_binary(fn, ies.NominalHafx, open_func)

def read_binary(fn: str, type_: type, open_func: Callable) -> list:
    sz = ctypes.sizeof(type_)
    def read_elt(f: IO[bytes]):
        d = type_()
        eof = (f.readinto(d) != sz)
        if eof: return None
        return d

    return generic_read_binary(fn, open_func, read_elt)

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

def main():
    p = argparse.ArgumentParser(
        description='Decode HaFX science files to JSON')
    p.add_argument(
        'files',
        help='files to decode to JSON')
    args = p.parse_args()

    fn = args.files

    hafx_data = read_hafx_sci(sys.stdin.buffer, gzip.GzipFile)
    time_deltas = [get_proper_timedelta(fn)] * len(hafx_data)
    data_type = [get_data_format(fn)] * len(hafx_data)

    jsonified = [hd.to_json() for hd in hafx_data]
    
    # Default value: start of UNIX epoch
    utc_time = dt.datetime.fromtimestamp(0, dt.UTC)
    for i, json_dat in enumerate(jsonified):
        frame_num = json_dat['buffer_number']['value']
        step = time_deltas[i]
        anchor = int(json_dat.pop('time_anchor')['value'])

        if anchor != 0:
            utc_time = dt.datetime.fromtimestamp(anchor, dt.UTC)
        json_dat['timestamp'] = {
            'value': int((utc_time + (frame_num % 32) * step).timestamp() * 1000),
            'unit': 'N/A'
        }
        type_ = data_type[i]
        json_dat['datatype'] = {
            'value': type_,
            'unit': 'N/A'
        }

    jsonified.sort(key=lambda e: e['timestamp']['value'])
    collapsed = collapse_json(jsonified)
    json_str = json.dumps(collapsed)
    sys.stdout.buffer.write(json_str.encode('utf-8'))


if __name__ == '__main__':
    main()