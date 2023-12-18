#!/usr/bin/env python3
# import sqlparse
import sys
import ast

if __name__ == "__main__":

    # filename = "example.txt"
    # query = "select * from table1, table2 where table1.name = table2.doe and table1.points = table2.points"
    # conditions = "name = doe and points = 150"

    filename1 = sys.argv[1]
    filename2 = sys.argv[2]
    conditions = sys.argv[3]

    conditions = conditions.split("and")
    cond_col_names1 = [x.split('=')[0].split('.')[1].rstrip().lstrip() for x in conditions]
    cond_col_names2 = [x.split('=')[1].split('.')[1].rstrip().lstrip() for x in conditions] # assuming cond is of form t1.col1 = t2.col2

    f = open(filename1, 'r')
    g = open(filename2, 'r')
    lines1 = f.readlines()
    lines2 = g.readlines()

    schema1 = lines1[0]
    schema2 = lines2[0]
    col_names1 = schema1.split(",")
    col_names1 = [x.rstrip("\n") for x in col_names1]
    col_names2 = schema2.split(",")
    col_names2 = [x.rstrip("\n") for x in col_names2]

    indices1 = [i for i, x in enumerate(col_names1) if x in cond_col_names1]
    indices2 = [i for i, x in enumerate(col_names2) if x in cond_col_names2]

    if len(lines1) > 1:
        for line in lines1[1:]:
            line = line.rstrip("\n")
            vals = line.split(",")
            vals = [x.lstrip().rstrip() for x in vals]

            ctr = 0
            for i in indices1:
                print(f"{vals[i]},(D1%{line})")

    if len(lines2) > 1:
        for line in lines2[1:]:
            line = line.rstrip("\n")
            vals = line.split(",")
            vals = [x.lstrip().rstrip() for x in vals]

            for i in indices2:
                print(f"{vals[i]},(D2%{line})")
            

    f.close()
    g.close()