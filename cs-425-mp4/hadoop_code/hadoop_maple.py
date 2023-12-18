#!/usr/bin/env python3
import sys

if __name__ == "__main__":

    # filename = "example.txt"
    # query = "select * from table where condition"
    # conditions = "name = doe and points = 150"

    # filename = sys.argv[1]
    X = sys.argv[1] # radio, fiber, radio/fiber, none
    # print(conditions)

    # cond_col_names = [x.split('=')[0].rstrip().lstrip() for x in conditions]
    # cond_vals = [x.split('=')[1].rstrip().lstrip() for x in conditions]

    # print(cond_col_names)
    # print(cond_vals)

    lines = []
    for line in sys.stdin:
        lines.append(line)

    # f = open(filename, 'r')
    # lines = f.readlines()

    schema = lines[0]
    col_names = schema.split(",")
    col_names = [q.rstrip("\n") for q in col_names]

    # print(col_names)
    index = [i for i, q in enumerate(col_names) if q == "Interconne"][0]
    index2 = [i for i, q in enumerate(col_names) if q == "Detection_"][0]

    if len(lines) > 1:
        for line in lines[1:]:
            line = line.rstrip("\n")
            vals = line.split(",")
            vals = [x.lstrip().rstrip() for x in vals]

            filtered_flag = 1
            if vals[index] != X:
                filtered_flag = 0

            if filtered_flag == 1:
                print(vals[index2]+",",str(1))

    # f.close()