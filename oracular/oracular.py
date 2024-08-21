import argparse
from collections import defaultdict
import os

parser = argparse.ArgumentParser()
parser.add_argument('-t', '--tracefilepath', type=str, required=True)
parser.add_argument('-o', '--output_dir', type=str, required=True)
parser.add_argument('-c', '--cross_cost', type=str, required=True)
args = parser.parse_args()
os.makedirs(args.output_dir, exist_ok=True)

hr_to_ms = 60 * 60 * 1000
gbToByte = 1024 * 1024 * 1024
capacity_cost = 0.023 / gbToByte / 30.5 / 24 / 3600  # per (byte * second)
if args.cross_cost == 'cross-cloud':
    cross_price = 0.09 / gbToByte
elif args.cross_cost == 'cross-region':
    cross_price = 0.02 / gbToByte
elif args.cross_cost == 'cross-10p':
    cross_price = 0.009 / gbToByte
elif args.cross_cost == 'cross-1p':
    cross_price = 0.0009 / gbToByte
else:
    raise NotImplementedError()

key_access_map = defaultdict(list)
with open(args.tracefilepath, 'r') as f:
    line_cnt = 0
    for line in f:
        line_cnt += 1
        
        if line.startswith('#'):
            continue
        
        splits = [v.strip() for v in line.split(',')]
        if len(splits) == 3:
            (ts, op, key), size = map(int, splits), 0
        elif len(splits) == 4:
            ts, op, key, size = map(int, splits)
        else:
            raise RuntimeError(f"Unexpected line format: {line}")
        
        key_access_map[key].append((ts, op, size))
last_ts = ts
max_hr = int(last_ts / hr_to_ms) + 1

print("# Start running offline optimal algorithm")

hr_to_dtc2app = [0.] * (max_hr + 1)
hr_to_dtc2dl = [0.] * (max_hr + 1)
hr_to_cc = [0.] * (max_hr + 1)            

def getDTC(size):
    return size * cross_price

def getCC(dur, size):
    return capacity_cost * size * dur / 1000.

def store_new(cts, nts, size, op):
    chr, nhr = int(cts / hr_to_ms) + 1, int(nts / hr_to_ms) + 1
    
    # capacity cost
    if chr == nhr:
        hr_to_cc[chr] += getCC(nts - cts, size)
    else:
        for i in range(chr, nhr + 1):
            hr_to_cc[i] += getCC(hr_to_ms * i - cts, size) if i == chr else (
                getCC(nts - hr_to_ms * (i - 1), size) if i == nhr else getCC(hr_to_ms, size))

    # data transfer cost (GET)
    if op == 1:
        hr_to_dtc2app[chr] += getDTC(size)

def store_continue(cts, nts, size):
    chr, nhr = int(cts / hr_to_ms) + 1, int(nts / hr_to_ms) + 1

    # capacity cost
    if chr == nhr:
        hr_to_cc[chr] += getCC(nts - cts, size)
    else:
        for i in range(chr, nhr + 1):
            hr_to_cc[i] += getCC(hr_to_ms * i - cts, size) if i == chr else (
                getCC(nts - hr_to_ms * (i - 1), size) if i == nhr else getCC(hr_to_ms, size))
            

def process_key(key, accesses):
    in_cache = False
    for i in range(len(accesses)):
        cts, cop, csize = accesses[i]
        chr = int(cts / hr_to_ms) + 1
        
        if cop == 0:  # current access is put
            assert not in_cache
            if i == len(accesses) - 1: # last access
                in_cache = False
            else:
                nts, nop, nsize = accesses[i + 1]
                if nop == 1: # next operation is GET
                    dur = nts - cts
                    if_store_cost = getCC(dur, csize)
                    if_not_store_cost = getDTC(csize)
                    if if_store_cost <= if_not_store_cost:
                        in_cache = True
                        store_new(cts, nts, csize, cop)
            hr_to_dtc2dl[chr] += getDTC(csize)

        elif cop == 1:   # current access is get
            if i == len(accesses) - 1: # last access
                if in_cache:
                    in_cache = False
                else:
                    hr_to_dtc2app[chr] += getDTC(csize)
            else:
                nts, nop, nsize = accesses[i + 1]
                if nop == 0: # next operation is PUT
                    if not in_cache:
                        hr_to_dtc2app[chr] += getDTC(csize)
                    in_cache = False
                elif nop == 1: # next operation is GET
                    dur = nts - cts
                    if_store_cost = getCC(dur, csize)
                    if_not_store_cost = getDTC(csize)
                    if if_store_cost <= if_not_store_cost:
                        if in_cache:
                            store_continue(cts, nts, csize)
                        else:
                            store_new(cts, nts, csize, cop)
                        in_cache = True
                    else:
                        if not in_cache:
                            hr_to_dtc2app[chr] += getDTC(csize)
                        in_cache = False
                            
                elif nop == 2: # next operation is DELETE
                    in_cache = False
                            
        elif cop == 2:   # current access is delete
            assert not in_cache
            
        else:
            raise RuntimeError("Not expected op type: " + str(cop))
        
for key, accesses in key_access_map.items():
    process_key(key, accesses)

outfilename = os.path.join(args.output_dir, "cost.csv")
with open(outfilename, 'w') as f:
    f.write("HR,DTC2APP,DTC2DL,CapacityCost\n")
    for hr in range(1, max_hr + 1):
        f.write("%d,%.6f,%.6f,%.6f\n" % (hr, hr_to_dtc2app[hr], hr_to_dtc2dl[hr], hr_to_cc[hr]))
        
