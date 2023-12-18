#!/bin/bash

for i in {700..700}
do
    dd if=/dev/urandom of=randomfile$i.txt bs="$((1024 * 1024))" count=$i
done
