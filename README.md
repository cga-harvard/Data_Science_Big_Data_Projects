# FASRC
Repository for FASRC projects.

## POSTGIS 

The following are instructions for installing POSTGIS on FASRC cluster:

### Request compute node
srun --pty --mem 16G -c 4  -p test -t 240 /bin/bash

### Once on the compute node, load the module
module load postgis/2.5.0-fasrc01

### Find an open port, start from some port number
j=5432while : ; do if /usr/sbin/lsof -Pi :$j -sTCP:LISTEN -t >/dev/null; then ((j++)); else break; fi; done

### Create the db data foldermkdir  
/scratch/$USER/$SLURM_JOB_IDDB_DATAFOLDER= /scratch/$USER/$SLURM_JOB_ID
initdb -D $DB_DATAFOLDER -U $USER

### Write your own conf file for POSTGIS

### Start the server
pg_ctl -D $DB_DATAFOLDER -P $j  -l logfile start

### Create db
createdb -h localhost -p $j -O $USER partisandb

### try to connect 
psql -h localhost -p $j partisandb


# OmniSci - Running without App
# Omniscio- Running with App

