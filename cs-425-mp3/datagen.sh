#!/bin/bash

for i in {500..500}
do
    dd if=/dev/urandom of=randomfile$i.txt bs="$((1024 * 1024))" count=$i
done
