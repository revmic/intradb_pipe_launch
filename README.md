#intradb_pipe_launch  
Pipeline launcher for IntraDB (Human Connectome Project internal site)  

----

###Prerequisites:  
python  
```
pip install requests
```
```
pip install envoy
```
OR, on the NRG cluster, run
```
source /nrgpackages/scripts/epd-python_setup.sh)  
```
hcpxnat (See below)

Run the install script to clone this repo and hcpxnat prerequisite.
```
$ ./install.sh  
```
update.sh is not recommended unless there is a known issue with the hcpxnat interface module.

###Usage Example:  
```
python intradbPipelineLauncher.py -u usr -p pass -H https://intradb.humanconnectome.org -s 100307_strc,100408_fnca -P HCP_Phase2 -i facemask
```

###Options:  
```
  -h, --help    show this help message and exit
  -u USERNAME, --username=USERNAME
  -p PASSWORD, --password=PASSWORD
  -H HOSTNAME, --hostname=HOSTNAME
  -s SESSIONS, --sessions=SESSIONS
  -P PROJECT, --project=PROJECT
  -i PIPELINE, --pipeline=PIPELINE
                        Options: validation, facemask, dcm2nii, or level2qc
  -r, --runOther        
  -o, --overwrite       Overwrite existing resources (assumed true for
                        facemask and dcm2nii)
  -U, --unusable        Try defacing unusable scans (skipped by default)
  -d DELAY, --delay=DELAY
                        Delay between pipeline launches in minutes
  -D, --dryRun          
```
