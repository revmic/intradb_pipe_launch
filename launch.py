#!/usr/bin/env python
from hcpxnat.interface import HcpInterface
from optparse import OptionParser
import datetime
import socket
import envoy
import time
import sys
import os

'''
Usage:
python intradbPipelineLauncher.py -u usr -p pss -H intradb.. -s 100307_strc,100408_fnca -P HCP_Phase2 -i facemask
'''
__author__ = "Michael Hileman"
__email__ = "hilemanm@mir.wuslt.edu"
__version__ = "1.0.1"

# TODO - 
# logging
# Right before launch, check again for resources?

parser = OptionParser(usage='\npython intradbPipelineLauncher.py -u usr -p pass ' +
    '-H https://intradb.humanconnectome.org -s 100307_strc,100408_fnca -P HCP_Phase2 -i facemask')
parser.add_option("-u", "--username", action="store", type="string", dest="username")
parser.add_option("-p", "--password", action="store", type="string", dest="password")
parser.add_option("-H", "--hostname", action="store", type="string", dest="hostname")
parser.add_option("-s", "--sessions", action="store", type="string", dest="sessions")
parser.add_option("-P", "--project", action="store", type="string", dest="project")
parser.add_option("-i", "--pipeline", action="store", type="string", dest="pipeline",
    help='Options: validation, facemask, dcm2nii, or level2qc')
parser.add_option("-r", "--runOther", action="store_true", dest="runOther", default=False)
parser.add_option("-o", "--overwrite", action="store_true", dest="overwrite", default=False,
    help='Overwrite existing resources (assumed true for facemask and dcm2nii)')

# Currently assume overwrite for facemask and dcm2nii
parser.add_option("-U", "--unusable", action="store_true", dest="unusable", default=False,
    help='Try defacing unusable scans (skipped by default)')
parser.add_option("-d", "--delay", action="store", type="string", dest="delay",
    help='Delay between pipeline launches in minutes')
parser.add_option("-D", "--dryRun", action="store_true", dest="dryRun", default=False)
# For existing resources, we currently assume Overwrite

(opts, args) = parser.parse_args()

if not opts.username:
    parser.print_help()
    sys.exit(-1)

idb = HcpInterface(opts.hostname, opts.username, opts.password, opts.project)

timestamp = datetime.datetime.now().strftime("%Y%m%d%M%s")

builddir = '/data/intradb/build/' + opts.project + '/' + timestamp
archivedir = '/data/intradb/archive/' + opts.project + '/arc001'

if not os.path.exists(builddir):
    os.makedirs(builddir)

runOtherParam = 'Y' if opts.runOther else 'N'

#######################################################
# Pipline versions
facemask_versions = {
    'HCP_Phase2':    'FaceMasking/FaceMasking.xml',
    'Phase2_Retest': 'FaceMasking/FaceMasking.xml',
    'Phase2_7T':     'FaceMaskingV2/FaceMaskingV2.xml',
    'LS_Phase1a':    'FaceMaskingV2/FaceMaskingV2.xml',
    'LS_Phase1b':    'FaceMaskingV2/FaceMaskingV2.xml',
    'LS_3T7T_1B':    'FaceMaskingV2/FaceMaskingV2.xml',
    'Mamah_CPC':     'FaceMaskingV2/FaceMaskingV2.xml'
    }

dcm2nii_versions = {
    'LS_3T7T_1B':    'HCP/HCPDefaceDicomToNifti2013.xml',
    'Mamah_CPC':     'HCP/HCPDefaceDicomToNifti2013.xml',
    'HCP_Phase2':    'HCP/HCPDefaceDicomToNifti.xml',
    'Phase2_Retest': 'HCP/HCPDefaceDicomToNifti.xml', 
    'Phase1_7T':     'HCP/HCPDefaceDicomToNifti.xml',
    'Phase2_7T':     'HCP/HCPDefaceDicomToNifti.xml',
    'LS_Phase1a':    'HCP/HCPDefaceDicomToNifti.xml',
    'LS_Phase1b':    'HCP/HCPDefaceDicomToNifti.xml',
    'Skyra_QC':      'HCP/HCPDefaceDicomToNifti.xml',
    'DMC_Phase1a':   'HCP/HCPDefaceDicomToNifti.xml',
    'Phase1Skyra':   'HCP/HCPDefaceDicomToNifti.xml'
    }

level2qc_xml = "HCP_QC_PARALLEL/Wrapper_QC/Level2QCLauncher_v2.0.xml"


def launchValidation():
    if 'hcpi' in socket.gethostname():
        cmd = '/data/intradb/pipeline/bin/PipelineJobSubmitter'
    else:
        cmd = ''
    print "cmd:", cmd

    print "idb.getSessionId():", idb.getSessionId()
    print "idb.session_label:", idb.session_label

    cmd +=' /data/intradb/pipeline/bin/XnatPipelineLauncher' + \
          ' -pipeline /data/intradb/pipeline/catalog/validation_tools/Validate.xml' + \
          ' -id ' + idb.getSessionId() + \
          ' -host ' + opts.hostname + \
          ' -u ' + opts.username + \
          ' -pwd ' + opts.password + \
          ' -dataType xnat:mrSessionData' + \
          ' -label ' + idb.session_label + \
          ' -supressNotification ' + \
          ' -project ' + idb.project + \
          ' -notify 2' + \
          ' -parameter mailhost=mail.nrg.wustl.edu' + \
          ' -parameter userfullname=A.Admin' + \
          ' -parameter adminemail=hilemanm@mir.wustl.edu' + \
          ' -parameter useremail=hilemanm@mir.wustl.edu' + \
          ' -parameter xnatserver=HCPIntradb' + \
          ' -parameter project=' + idb.project + \
          ' -parameter sessionType=xnat:mrSessionData' + \
          ' -parameter xnat_id=' + idb.getSessionId() + \
          ' -parameter sessionId=' + idb.session_label + \
          ' -parameter archivedir=' + archivedir + \
          ' -parameter builddir=' + builddir + \
          ' -parameter xnat_project=' + idb.project + \
          ' -parameter catalog_content=v0.1' + \
          ' -parameter session=' + idb.getSessionId() + \
          ' -parameter sessionLabel=' + idb.session_label

    print cmd

    if not opts.dryRun:
        p = envoy.run(cmd)
        print p.std_out
        print p.std_err

def launchFacemask():
    # We now have two different versions of the pipeline used for different 
    # projects. -Hileman 2014-9-23
    # TODO: Refactor how pipeline version and session suffix is handled
    try:
        facemask_xml = facemask_versions[idb.project]
    except KeyError:
        print idb.project, "not in facemask_versions map. Assuming FaceMaskingV2.xml ..."
        facemask_xml = 'FaceMaskingV2/FaceMaskingV2.xml'

    if idb.project == 'Phase2_7T':
        ref_project = 'HCP_Phase2'
    else:
        ref_project = idb.project

    ref_session = getReferenceSession(ref_project)
    if not ref_session:
        return

    ref_scan_id = getReferenceScan(ref_project, ref_session)
    # TODO - getReferences(): combine all reference gets
    scan_ids = filterScanIds()

    print "Facemask xml:", facemask_xml
    print "idb session id:", idb.getSessionId()
    print "idb subject id:", idb.getSubjectId()
    print "ref session:", ref_session
    print "ref scanid:", ref_scan_id

    # Don't use the PipelineJobSubmitter if launching from any node other than
    # hcpi-fs01 or hcpi-dev VMs. This causes an issue when launching from the
    # interface and autorun since it will try to submit the job from a node
    # that isn't allowed. -Hileman 2014-11-06
    if 'hcpi' in socket.gethostname():
        cmd = '/data/intradb/pipeline/bin/PipelineJobSubmitter'
    else:
        cmd = ''

    #cmd = '/data/intradb/pipeline/bin/PipelineJobSubmitter' + \
    cmd +=' /data/intradb/pipeline/bin/XnatPipelineLauncher' + \
          ' -pipeline /data/intradb/pipeline/catalog/' + facemask_xml + \
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
          ' -parameter scanids=' + ",".join(scan_ids) + \
          ' -parameter subject=' + idb.getSubjectId() + \
          ' -parameter usebet=1' + \
          ' -parameter maskears=1' + \
          ' -parameter invasiveness=1.0' + \
          ' -parameter threshold=-1' + \
          ' -parameter ref_session=' + ref_session + \
          ' -parameter ref_scan_type=T1w' + \
          ' -parameter ref=' + ref_scan_id + \
          ' -parameter use_manual_roi=0' + \
          ' -parameter rois=0' + \
          ' -parameter dirs=0' + \
          ' -parameter existing=Overwrite' + \
          ' -parameter runOtherPipelines=' + runOtherParam

    # TODO - Get rid of hard coding
    if idb.project == 'Phase2_7T':
        cmd += ' -parameter ref_project=HCP_Phase2'
    if '7T' in idb.session_label:
        cmd += ' -parameter other_flags_and_args=-am'

    print "\n=========================="
    print ''
    print cmd

    if not opts.dryRun:
        p = envoy.run(cmd)
        print p.std_out
        print p.std_err

def filterScanIds():
    """
    If the -U option isn't set, throw out any scans marked unusable since 
    they're often problematic and shouldn't end up on Connectomedb
    """
    scans = idb.getSessionScans()
    scan_ids = list()

    for scan in scans:
        # don't process scan if it is unusable and the unusable param isn't set
        if scan['quality'] == 'unusable' and not opts.unusable:
            continue
        else:
            scan_ids.append(scan['ID'])
    return scan_ids

def getReferenceSession(ref_project):
    """
    Lifespan subjects could have Structural scans in different sessions.
    This will go through the sessions making sure it can find a good T1w
    """
    # Save a reference to the session and project being facemasked
    facemask_session_label = idb.session_label
    facemask_project = idb.project
    idb.project = ref_project
    sessions = idb.getSubjectSessions()

    # Set some defaults in case nothing is found
    if 'LS_Phase' in idb.project:
        ref_session = idb.subject_label + '_V1_A'
    elif 'Mamah' in idb.project:
        ref_session = idb.subject_label + '_A2'
    elif 'Phase2_Retest' in idb.project:
        ref_session = idb.subject_label + '_strc_rt'
    else:
        ref_session = idb.subject_label + '_strc'

    # First pass doesn't consider usability for the sake of autoruns
    for s in sessions:
        idb.session_label = s['label']
        scans = idb.getSessionScans()

        for scan in scans:
            if scan['type'] == 'T1w':
                ref_session = idb.session_label
                break

    # Second pass considers usability
    for s in sessions:
        idb.session_label = s['label']
        scans = idb.getSessionScans()
        
        for scan in scans:
            if scan['type'] == 'T1w' and \
                (scan['quality'] == 'good' or scan['quality'] == 'usable'):
                ref_session = idb.session_label
                break

    if not idb.experimentExists(ref_session):
        print "\nReference session", ref_session, "in", ref_project, "not found."
        print "Skipping ...\n"
        # Set to None so facemask method knows to skip
        ref_session = None

    # Set the intradb instance variable back to the session and project being facemasked
    idb.session_label = facemask_session_label
    idb.project = facemask_project

    return ref_session


def getReferenceScan(ref_project, ref_session):
    """
    Make one pass through scans and set ref to the first T1w
    Make a second pass and set to first 'good' quality if it exists
    Initialize the ref scan to 'NONE' in the event 1&2 don't find anything
    """
    # Save a reference to the session and project being facemasked
    facemask_session_label = idb.session_label
    facemask_project = idb.project
    # Set idb instance variable so we can perform actions on our idb object
    idb.session_label = ref_session
    idb.project = ref_project
    scans = idb.getSessionScans()
    ref_scan_id = 'NONE' # Let pipeline figure it out if nothing found

    # First pass grabs the first T1w
    for scan in scans:
        if scan['type'] == 'T1w':
            ref_scan_id = scan['ID']
            break

    # Second pass picks anything but unusable, mostly useful for cases where 
    # some scans are set to unusable while others are left undeterminted
    for scan in scans:
        if scan['type'] == 'T1w' and (scan['quality'] != 'unusable'):
            ref_scan_id = scan['ID']
            break


    # Third pass considers usability
    for scan in scans:
        if scan['type'] == 'T1w' and \
                (scan['quality'] == 'good' or scan['quality'] == 'usable'):
            ref_scan_id = scan['ID']
            break

    # Set the intradb instance variable back to the session and project being facemasked
    idb.session_label = facemask_session_label
    idb.project = facemask_project
    return ref_scan_id

def launchDicomToNifti():
    try:
        dcm2nii_xml = dcm2nii_versions[idb.project]
    except KeyError:
        print idb.project, "not in dcm2nii_versions map. Assuming HCPDefaceDicomToNifti2013.xml ..."
        dcm2nii_xml = 'HCP/HCPDefaceDicomToNifti2013.xml'

    cmd = '/data/intradb/pipeline/bin/PipelineJobSubmitter' + \
          ' /data/intradb/pipeline/bin/XnatPipelineLauncher' + \
          ' -pipeline /data/intradb/pipeline/catalog/' + dcm2nii_xml + \
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
          ' -parameter runOtherPipelines=' + runOtherParam + \
          ' -parameter notify=0'

    print cmd
    if not opts.dryRun:
        p = envoy.run(cmd)
        print p.std_out
        print p.std_err
        #print p.status_code

def launchLevel2QC():
    if opts.overwrite:# and not opts.dryRun:
        deleteExistingQC()
        
    cmd = '/data/intradb/pipeline/bin/PipelineJobSubmitter' + \
          ' /data/intradb/pipeline/bin/XnatPipelineLauncher' + \
          ' -pipeline /data/intradb/pipeline/catalog/' + level2qc_xml + \
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
          ' -parameter functional_scan_type=rfMRI,tfMRI' + \
          ' -parameter fieldMap_scan_type=FieldMap'

    print cmd
    if not opts.dryRun:
        p = envoy.run(cmd)
        print p.std_out
        print p.std_err
        #print p.status_code

def deleteExistingQC():
    print "Deleting QC Assessments for", idb.session_label
    assessments = idb.getSessionAssessors()
    qc_assessments = []

    for ass in assessments:
        if 'qcAssessment' in ass['xsiType']:
            qc_assessments.append(ass)

    if not qc_assessments:
        print "No existing Level2QC assessments to delete"

    for qc_ass in qc_assessments:
        idb.deleteSessionAssessor(qc_ass['label'])

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
        idb.subject_label = idb.getSessionSubject()
        # TODO - fails sometimes, so check that this found something
        if not idb.subject_label:
            idb.subject_label = s.split('_')[0]  # Not general, but often works
        print '\n** Subject: ' + idb.subject_label
        print '** Session: ' + s + '\n'

        if opts.pipeline == 'validation':
            launchValidation()
        elif opts.pipeline == 'facemask':
            launchFacemask()
        elif opts.pipeline == 'dcm2nii':
            launchDicomToNifti()
        elif opts.pipeline == 'level2qc':
            # Set default delay for level2qc
            if not opts.delay:
                opts.delay = '2'
            launchLevel2QC()
        else:
            print "Unknown Intradb pipeline: " + opts.pipeline

        if opts.delay:
            print "================================="
            print "Sleeping for", opts.delay, "minutes ..."
            print "================================="
            time.sleep(float(opts.delay) * 60)

