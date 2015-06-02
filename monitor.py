from __future__ import print_function
from hcpxnat.interface import HcpInterface
from optparse import OptionParser
from datetime import datetime
import socket
import envoy
import time
import sys
import os

parser = OptionParser()
parser.add_option("-s", "--sessions", action="store", type="string", dest="sessions")
parser.add_option("-p", "--project", action="store", type="string", dest="project",
                  help="Not yet implemented")
parser.add_option("-u", "--sge-user", action="store", type="string", dest="user",
                  help="Not yet implemented")
(opts, args) = parser.parse_args()
if not (opts.sessions or opts.project or opts.user):
    #print('Monitoring all pipeline processes for hcpinternal ...')
    #time.sleep(3)
    parser.print_help()
    sys.exit(-1)

idb = HcpInterface(config='/data/intradb/home/hileman/.hcpxnat_intradb.cfg')
idb.project = opts.project
sessions_list = []

def get_sessions_list():
    sessions = idb.getSessions(project=idb.project)
    for s in sessions:
        sessions_list.append(s['label'])

if opts.sessions:
    sessions_list = opts.sessions.split(',')
    sessions_str = opts.sessions.replace(',', '|')

def monitor_sessions():
    """
    Assumes hcpinternal user
    """
    qstat_cmd = "qstat -u 'hcpinternal' -j '*'"
    r = envoy.run(qstat_cmd)
    
    job_args = r.std_out.split('==============================================================')
    if len(job_args) == 0:
        print("\nNo jobs submitted or running for session list. Exiting...")
        sys.exit(0)

    os.system('clear')
    #sys.stdout.flush()
    now = str(datetime.now())
    #sys.stdout.write("time: %s -- %s total jobs running on %s\n" % \
    #     (now.split()[1].split('.')[0], len(job_args), socket.gethostname()))
    output = "last updated: " + str(now.split()[1].split('.')[0])
    output += "\n=============================================================\n"
    output += "Submitted\t\tSession\t\tPipeline\n"
    output += "=============================================================\n"

    proc_count = 0
    part_count = 0
    for p in job_args:
        for s in sessions_list:
            if ('-label,' + s in p) or (s + '/Level2QC' in p):
                # prepend/append strings to weed out duplicates, 
                # e.g., from ref_session or in path names
                proc_count += 1
                proc_parts = p.split()
                st = proc_parts[5:9]
                submit_time = "%s - %s %s %s" % (st[3], st[0], st[1], st[2])
                pipeline = "Unknown"
        
                for part in proc_parts:
                    if '-pipeline' in part:
                        launch_command = part
                        pipeline = part.split(',')[1].split('/')[-1].replace('.xml', '')
                    elif 'Wrapper_QC' in part:
                        pipeline = "Level2QC subproc"
                    #print part_count, part
                    part_count += 1
        
                line = "%s\t%s\t%s\n" % (submit_time, s, pipeline) 
                output += line
                #sys.exit()

    output += "=============================================================\n"
    output += "%s total jobs running on %s\n" % (len(job_args), socket.gethostname())
    output += str(proc_count) + ' monitored jobs running'
    print(output)
 
def monitor_user():
    qstat_cmd = "qstat -u '%s' -j '*'" % opts.user
    r = envoy.run(qstat_cmd)

    job_args = r.std_out.split('==============================================================')
    #for p in job_args:
    #    print p

    if len(job_args) == 0:
        print("\nNo jobs submitted or running for this user. Exiting...")
        sys.exit(0)
   
    os.system('clear')
    now = str(datetime.now())

    sys.stdout.write("time: %s -- %s total jobs running on %s\n" % \
         (now.split()[1].split('.')[0], len(job_args), socket.gethostname()))
    sys.stdout.write("\n=============================================================\n")
    sys.stdout.write("Submitted\t\tSession\t\tPipeline\n")
    sys.stdout.write("=============================================================\n\n")

    running_jobs = 0
    for p in job_args:
        print(p)
        proc_parts = p.split()
        print(proc_parts)
        st = proc_parts[5:9]
        submit_time = "time" # "%s - %s %s %s" % (st[3], st[0], st[1], st[2])

        for part in proc_parts:
            if '-parameter' in part:
                print(part)
                launch_command = part
                pipeline = part.split(',')[1].split('/')[-1].replace('.xml', '')
                session_label = part.split(',')[13]
                running_jobs += 1

        line = "%s\t%s\t%s\n" % (submit_time, "session_label", pipeline)
        #sys.stdout.write(line)

sys.stdout.write("\n=============================================================\n")

if __name__ == '__main__':
    if opts.project:
        get_sessions_list()

    while True:
        #if opts.sessions or opts.project:
        monitor_sessions()
        #elif opts.project:
        #    monitor_sessions()
        #elif opts.user:
        #    monitor_user()
        time.sleep(30)
