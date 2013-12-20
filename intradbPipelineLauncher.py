#!/usr/bin/env python
from hcpxnat.interface import HcpInterface
from optparse import OptionParser
import datetime
import envoy
import sys
import os

'''
Usage:
python intradbPipelineLauncher.py -u usr -p pss -H intradb.. -s 100307_strc,100408_fnca -P HCP_Phase2 -i facemask
'''
# TODO - 
# logging
# Right before launch, check again for resources?

__author__ = "Michael Hileman"
__email__ = "hilemanm@mir.wuslt.edu"
__version__ = "0.8.1"

parser = OptionParser(usage='\npython intradbPipelineLauncher.py -u usr -p pass ' +
    '-H https://intradb.humanconnectome.org -s 100307_strc,100408_fnca -P HCP_Phase2 -i facemask')
parser.add_option("-u", "--username", action="store", type="string", dest="username")
parser.add_option("-p", "--password", action="store", type="string", dest="password")
parser.add_option("-H", "--hostname", action="store", type="string", dest="hostname")
parser.add_option("-s", "--sessions", action="store", type="string", dest="sessions")
parser.add_option("-P", "--project", action="store", type="string", dest="project")
parser.add_option("-i", "--pipeline", action="store", type="string", dest="pipeline",
    help='Options: validation, facemask, dcm2nii, or level2qc')
#parser.add_option("-e", "--existing", action="store", type="string", dest="existing")
#parser.add_option("-o", "--runOther", action="store", type="string", dest="project")
(opts, args) = parser.parse_args()

if not opts.username:
    parser.print_help()
    sys.exit(-1)

idb = HcpInterface(opts.hostname, opts.username, opts.password, opts.project)

timestamp = datetime.datetime.now().strftime("%Y%m%d%M%s")
builddir = '/data/intradb/build1/'+opts.project+'/' + timestamp
archivedir = '/data/intradb/archive1/'+opts.project+'/arc001'

if not os.path.exists(builddir):
    os.makedirs(builddir)

def launchValidation():
    print "Protocol Validation not yet implemented."

def launchFacemask():
    cmd = '/data/intradb/pipeline/bin/PipelineJobSubmitter' + \
          ' /data/intradb/pipeline/bin/XnatPipelineLauncher' + \
          ' -pipeline /data/intradb/pipeline/catalog/FaceMasking/FaceMasking.xml' + \
          ' -id ' + idb.getSessionId() + \
          ' -host ' + opts.hostname + \
          ' -u ' + opts.username + \
          ' -pwd ' + opts.password + \
          ' -dataType xnat:mrSessionData' + \
          ' -label ' + idb.session_label + \
          ' -supressNotification ' + \
          ' -project ' + idb.project + \
          ' -notify hilemanm@mir.wustl.edu' + \
          ' -parameter mailhost=mail.nrg.wustl.edu ' + \
          ' -parameter userfullname=Admin' + \
          ' -parameter builddir=' + builddir + \
          ' -parameter xnatserver=HCPIntradb' + \
          ' -parameter adminemail=hilemanm@mir.wustl.edu' + \
          ' -parameter useremail=hilemanm@mir.wustl.edu' + \
          ' -parameter xnat_id=' + idb.getSessionId() + \
          ' -parameter sessionId=' + idb.session_label + \
          ' -parameter archivedir=' + archivedir + \
          ' -parameter project=' + idb.project + \
          ' -parameter scanids=' + ",".join(idb.getSessionScanIds()) + \
          ' -parameter subject=' + idb.getSubjectId() + \
          ' -parameter usebet=1' + \
          ' -parameter maskears=1' + \
          ' -parameter invasiveness=1.0' + \
          ' -parameter threshold=-1' + \
          ' -parameter ref_session=strc' + \
          ' -parameter ref_scan_type=T1w' + \
          ' -parameter ref=NONE' + \
          ' -parameter use_manual_roi=0' + \
          ' -parameter rois=0' + \
          ' -parameter dirs=0' + \
          ' -parameter existing=Overwrite' + \
          ' -parameter runOtherPipelines=N'

    print cmd

    p = envoy.run(cmd)
    print p.std_out
    print p.std_err
    print p.status_code
    # do someting with std_out/err
    # return codes??

def launchDicomToNifti():
    cmd = '/data/intradb/pipeline/bin/PipelineJobSubmitter' + \
          ' /data/intradb/pipeline/bin/XnatPipelineLauncher' + \
          ' -pipeline /data/intradb/pipeline/catalog/HCP/HCPDefaceDicomToNifti.xml' + \
          ' -id ' + idb.getSessionId() + \
          ' -host ' + opts.hostname + \
          ' -u ' + opts.username + \
          ' -pwd ' + opts.password + \
          ' -dataType xnat:mrSessionData' + \
          ' -label ' + idb.session_label + \
          ' -project ' + idb.project + \
          ' -notify hilemanm@mir.wustl.edu' + \
          ' -parameter mailhost=mail.nrg.wustl.edu ' + \
          ' -parameter userfullname=Admin' + \
          ' -parameter builddir=' + builddir + \
          ' -parameter xnatserver=HCPIntradb' + \
          ' -parameter adminemail=hilemanm@mir.wustl.edu' + \
          ' -parameter useremail=hilemanm@mir.wustl.edu' + \
          ' -parameter xnat_id=' + idb.getSessionId() + \
          ' -parameter sessionId=' + idb.session_label + \
          ' -parameter archivedir=' + archivedir + \
          ' -parameter project=' + idb.project + \
          ' -parameter scanids=' + ",".join(idb.getSessionScanIds()) + \
          ' -parameter subject=' + idb.getSubjectId() + \
          ' -parameter create_nii=Y' + \
          ' -parameter keep_qc=N' + \
          ' -parameter overwrite_existing=Y' + \
          ' -parameter runOtherPipelines=N' + \
          ' -parameter notify=0'

    print cmd
    p = envoy.run(cmd)
    print p.std_out
    print p.std_err
    print p.status_code

def launchLevel2QC():
    cmd = '/data/intradb/pipeline/bin/PipelineJobSubmitter' + \
          ' /data/intradb/pipeline/bin/XnatPipelineLauncher' + \
          ' -pipeline /data/intradb/pipeline/catalog/HCP_QC/Wrapper_QC/Level2QCLauncher_v1.0.xml' + \
          ' -id ' + idb.getSessionId() + \
          ' -host ' + opts.hostname + \
          ' -u ' + opts.username + \
          ' -pwd ' + opts.password + \
          ' -dataType xnat:mrSessionData' + \
          ' -label ' + idb.session_label + \
          ' -project ' + idb.project + \
          ' -notify hilemanm@mir.wustl.edu' + \
          ' -parameter mailhost=mail.nrg.wustl.edu ' + \
          ' -parameter userfullname=Admin' + \
          ' -parameter builddir=' + builddir + \
          ' -parameter xnatserver=HCPIntradb' + \
          ' -parameter adminemail=hilemanm@mir.wustl.edu' + \
          ' -parameter useremail=hilemanm@mir.wustl.edu' + \
          ' -parameter xnat_id=' + idb.getSessionId() + \
          ' -parameter sessionId=' + idb.session_label + \
          ' -parameter archivedir=' + archivedir + \
          ' -parameter project=' + idb.project + \
          ' -parameter structural_scan_type=T1w,T2w' + \
          ' -parameter functional_scan_type=rfMRI,tfMRI' + \
          ' -parameter diffusion_scan_type=dMRI'

    print cmd
    p = envoy.run(cmd)
    print p.std_out
    print p.std_err
    print p.status_code


if __name__ == "__main__":
    sessions = opts.sessions.split(',')
    print "=" * 82
    print "Launching %s pipeline for %s sessions on %s" % \
    (opts.pipeline, sessions.__len__(), idb.url)
    print "Build directory: " + builddir
    print "Archive directory: " + archivedir
    print "=" * 82

    for s in sessions:
        idb.session_label = s
        idb.subject_label = s.split('_')[0]  # idb.getSessionSubject()
        print '\n** Session: ' + s + '\n'

        if opts.pipeline == 'validation':
            launchValidation()
        elif opts.pipeline == 'facemask':
            launchFacemask()
        elif opts.pipeline == 'dcm2nii':
            launchDicomToNifti()
        elif opts.pipeline == 'level2qc':
            launchLevel2QC()
        else:
            print "Unknown Intradb pipeline: " + opts.pipeline
