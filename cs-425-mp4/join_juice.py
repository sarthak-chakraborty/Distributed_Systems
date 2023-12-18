#!/usr/bin/env python3
import sys
import ast

if __name__ == "__main__":

    # filename = "prefix_Key.txt"

    filename = sys.argv[1]
    key = filename.split('_', 1)[1]

    with open(filename, "r") as f:
        lines = f.readlines()
        
    dataset = []
    for line in lines:
        line = line.rstrip("\n")
        tokens = line.split('%')
        d = tokens[0][1:]
        dataset.append(d)

    if "D1" in dataset and "D2" in dataset:
        for i in range(len(lines)):
            line1 = lines[i]
            line1 = line1.rstrip("\n")
            tokens = line1.split('%')

            for j in range(i, len(lines)):
                line2 = lines[j]
                line2 = line2.rstrip("\n")
                tokens2 = line2.split('%')
                if tokens[0] != tokens2[0]:
                    print(f"{tokens[1].split(')')[0]},{tokens2[1].split(')')[0]}")

        

