#!/usr/bin/env python3
import sys
import ast
from collections import defaultdict

if __name__ == "__main__":

    # filename = "prefix_Key.txt"

    # filename = sys.argv[1]
    # key = filename.split('_')[1] 

    # with open(filename, "r") as f:
    #     lines = f.readlines()
    #     print(key, len(lines))
    lines = []
    key_freq = defaultdict(int)

    for line in sys.stdin:
            # print(line)
        key = line.split(',')[0]
        key_freq[key] += 1
    
    for key in key_freq.keys():
        print("1,{} {}".format(key, key_freq[key]))
