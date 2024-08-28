import argparse
import json
import gzip
import sys
import struct
from typing import Any, Callable, IO

import umndet.common.impress_exact_structs as ies

def generic_read_binary(
    fn: str,
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

def read_x123_sci(fn: str) -> list[ies.X123NominalSpectrumStatus]:
    def read_elt(f: IO[bytes]):
        timestamp, = struct.unpack('<L', f.read(4))
        status_bytes = f.read(64)
        spectrum_size, = struct.unpack('<H', f.read(2))
        spectrum = list(struct.unpack('<' + ('L' * spectrum_size), f.read(4 * spectrum_size)))
        return ies.X123NominalSpectrumStatus(
            timestamp, spectrum, status_bytes
        )
    return generic_read_binary(fn, read_elt)

def main():
    x123_data = read_x123_sci(sys.stdin.buffer)

    json_out = [xd.to_json() for xd in x123_data]
    json_out.sort(key=lambda e: e['timestamp'])
    json_str = json.dumps(json_out)
    sys.stdout.buffer.write(json_str.encode('utf-8'))

if __name__ == '__main__':
    main()