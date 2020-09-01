#!/bin/bash

for dir in /n/holyscratch01/cga/jblossom/PRISM_data/tmax/daily/*;
do
    #echo "$dir"
    #dir1=/n/holyscratch01/cga/dkakkar/data/BIL/output
    IFS='/' # space is set as delimiter
    read -ra ADDR <<< "$dir" # str is read into an array as tokens separated by IFS
    i="${ADDR[-1]}"
    echo "$i"
    cmd1="raster2pgsql -d -I -C -M -F -t 100x100 -s 4269 $dir/*.bil tmax$i > tmax$i.sql"
    #echo "$cmd1"
    eval "$cmd1"
    cmd2="psql -h localhost -p 7779 -d postgres -f tmax$i.sql"
    #echo "$cmd2"
    eval "$cmd2"
    cmd3="rm *.sql"
    eval "$cmd3"
done

