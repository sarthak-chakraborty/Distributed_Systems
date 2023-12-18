#!/usr/bin/env python3
import sys
import ast 

tot = 0
keys = {}
for line in sys.stdin:
    key = line.split(',')[1].split(' ')[0]
    val = int(line.split(',')[1].split(' ')[1])
    tot += val
    keys[key] = val

for key in keys:
    print(key, (keys[key]/tot)*100)

