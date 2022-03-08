#!/bin/bash
module load postgis
module load Anaconda3/5.0.1-fasrc02
source activate postgis
python3 /n/holyscratch01/cga/dkakkar/scripts/restore_dumps.py 
