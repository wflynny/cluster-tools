#!/usr/bin/env python
from __future__ import unicode_literals
import re
import os
import pwd, grp
import argparse
from subprocess import check_call, check_output, Popen, PIPE

def find_last_jobid(user):
    content = filter(None, check_output(['qstat', '-u', user]).split('\n'))
    if not content:
        exit("No running jobs found")
    return content[-1].split()[0].split('.')[0]

def find_node(jobid):
    content = check_output(['checkjob', jobid])
    match = re.search(r'^\[([A-z0-9\-\:]+)\]$', content, re.M)
    if match:
        node = match.group(1).split(':')[0]
    else:
        raise Exception("Couldn't parse `checkjob %s` for node", jobid)
    return node

def main(args):
    if args.last:
        jobid = find_last_jobid(pwd.getpwuid(os.getuid()).pw_name)
    else:
        jobid = str(args.jobid)

    node = find_node(jobid)

    print "ssh", node
    check_call(['history', '-s', 'ssh', 'node'], shell=True)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-l', '--last', action='store_true')
    group.add_argument('jobid', type=int, nargs='?')
    main(parser.parse_args())
