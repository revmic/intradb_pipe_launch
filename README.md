intradbpipe
===========

Pipeline launcher for IntraDB

Prerequisites:
python
requests module
envoy module
 (on NRG cluster, source /nrgpackages/scripts/epd-python_setup.sh)
hcpxnat (Installation)

Installation:
(need to put this in install.sh)
rmdir hcpxnat
git clone https://github.com/revmic/hcpxnat

Usage: 
python intradbPipelineLauncher.py -u usr -p pass -H https://intradb.humanconnectome.org -s 100307_strc,100408_fnca -P HCP_Phase2 -i facemask

Options:
  -h, --help   show this help message and exit
  -u USERNAME, --username=USERNAME
  -p PASSWORD, --password=PASSWORD
  -H HOSTNAME, --hostname=HOSTNAME
  -s SESSIONS, --sessions=SESSIONS
  -P PROJECT,  --project=PROJECT
  -i PIPELINE, --pipeline=PIPELINE
               Options: validation, facemask, dcm2nii, or level2qc