#!/usr/bin/env python3
import sys

if __name__ == "__main__":


    filename = sys.argv[1]

    f = open(filename, 'r')
    lines = f.readlines()

    for line in lines:
        line = line.rstrip("\n")
        print(f"1,({line})")