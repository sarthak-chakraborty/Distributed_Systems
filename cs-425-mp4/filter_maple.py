#!/usr/bin/env python3
# import sqlparse
import sys
import ast
import re

def sql_parser(query):
    parsed = sqlparse.parse(query)
    # parsed is a list of SQL statements
    stmt = parsed[0]

    # Get the WHERE clause token
    where_clause = None
    for token in stmt.tokens:
        if token.ttype is None and isinstance(token, sqlparse.sql.Where):
            where_clause = token
            break

    # Extract and print the conditions from the WHERE clause
    if where_clause is not None:
        conditions = []
        for token in where_clause.tokens:
            if isinstance(token, sqlparse.sql.Comparison):
                conditions.append(str(token))

    return conditions


if __name__ == "__main__":

    # filename = "example.txt"
    # query = "select * from table where name = doe and points = 150"
    # conditions = "name = doe and points = 150"

    filename = sys.argv[1]
    conditions = sys.argv[2]

    conditions = conditions.split("and")

    cond_col_names = [x.split('=')[0].rstrip().lstrip() for x in conditions]
    cond_vals = [x.split('=')[1].rstrip().lstrip() for x in conditions]

    f = open(filename, 'r')
    lines = f.readlines()

    schema = lines[0]
    col_names = schema.split(",")
    col_names = [x.rstrip("\n") for x in col_names]

    # print(col_names)
    indices = [i for i, x in enumerate(col_names) if x in cond_col_names]

    if len(lines) > 1:
        for k, line in enumerate(lines[1:]):
            line = line.rstrip("\n")
            vals = line.split(",")
            vals = [x.lstrip().rstrip() for x in vals]

            ctr = 0
            filtered_flag = 1
            for i in indices:
                if not re.match(cond_vals[ctr], vals[i]):
                    # print(cond_vals[ctr], vals[i])
                    filtered_flag = 0
                    break
                # if vals[i] != cond_vals[ctr]:
                #     filtered_flag = 0
                #     break
                # print(k+2, cond_vals[ctr], vals[i])

                ctr += 1
            if filtered_flag == 1:
                print(vals[0]+","+str(filtered_flag))


    f.close()