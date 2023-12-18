#!/usr/bin/env python3
import sys
import ast

if __name__ == "__main__":

    # filename = "prefix_Key.txt"

    filename = sys.argv[1]
    key = filename.split('_',1)[1] 

    with open(filename, "r") as f:
        lines = f.readlines()
        print(f"\"{key}\",{len(lines)}")
