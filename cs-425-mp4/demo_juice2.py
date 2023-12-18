#!/usr/bin/env python3
import sys
import ast

if __name__ == "__main__":

    # filename = "prefix_Key.txt"

    filename = sys.argv[1]
    key = filename.split('_')[1] 

    with open(filename, "r") as f:
        lines = f.readlines()
        
    total = 0
    for line in lines:
        line = line.rstrip("\n")
        tup = eval(line)
        total += tup[1]

    for line in lines:
        line = line.rstrip("\n")
        tup = eval(line)
        print(f"{tup[0]},{tup[1]/total}")
