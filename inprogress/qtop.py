#!/usr/bin/env python
"""
Customizable qstat script because the default qstat sucks
"""
import re, os, sys
import pwd, grp
import getpass
import signal
from xml.etree import ElementTree
import socket
import numpy as np
from numpy import uint8, uint16, uint32
import time
import pandas as pd
from Queue import Queue
import termios
import tty
try:
    import argparse
    from subprocess import check_output
except:
    print "\n\tError:",
    print "\tThis script requires at least Python 2.7 for basic functionailty."
    print "\t\tUse one of the available python modules (anaconda or opt-python)"
    print "\t\tor use my conda-pyopencl module in /home/tuf31071/privatemodules\n"
    sys.exit(1)

allowed_args = {'id':                       ['{:^6.6}',"JobID"],
                'job_name':                 ['{:<13.13}',"Jobname"],
                'job_owner':                ['{:<8.9}',"Username"],
                'job_owner_full':           ['{:<17.17}',"Fullname"],
                'job_owner_group':          ['{:<5.5}',"Group"],
                'resources_used.cput':      ['{:<8}',"CPUtime"],
                'resources_used.mem':       ['{:<8}',"Mem Used"],
                'resources_used.vmem':      ['{:<8}',"VMem Used"],
                'resources_used.walltime':  ['{:^6}',"ETime"],
                'job_state':                ['{:^1}',"S"],
                'priority':                 ['{:>5}',"PrioT"],
                'queue':                    ['{:<7}',"Queue"],
                'gpuct':                    ['{:^4}',"GPUs"],
                'cpuct':                    ['{:^4}',"CPUs"],
                'exec_gpus':                ['{:<20.20}',"GPUs"],
                'qtime':                    ['{:<10}',"Queue Time"],
                'resource_list.nodect':     ['{:>3}',"# N"],
                'resource_list.nodes':      ['{:<}',"Node args"],
                'resource_list.walltime':   ['{:^6.6}',"RTime"]}

update_args = {'resources_used.cput':      ['{:<8}',"CPUtime"],
               'resources_used.mem':       ['{:<8}',"Mem Used"],
               'resources_used.vmem':      ['{:<8}',"VMem Used"],
               'resources_used.walltime':  ['{:^6}',"ETime"],
               'job_state':                ['{:^1}',"S"],
               'priority':                 ['{:>5}',"PrioT"],
               'gpuct':                    ['{:^4}',"GPUs"],
               'cpuct':                    ['{:^4}',"CPUs"],
               'exec_gpus':                ['{:<20.20}',"GPUs"],
               'qtime':                    ['{:<10}',"Queue Time"],
               'resource_list.nodect':     ['{:>3}',"# N"],
               'resource_list.nodes':      ['{:<}',"Node args"],
               'resource_list.walltime':   ['{:^6.6}',"RTime"]}

class Empty(object):
    def __getattribute__(self, name):
        return ""

class MauiSocket(object):
    @staticmethod
    def _DoCRC(crc, onech):
        assert(type(crc) is uint16 and type(onech) is uint8)
        ans = uint32(crc ^ onech << uint16(8))
        for ind in range(8):
            if ans & uint32(0x8000):
              ans <<= uint32(1)
              ans = ans ^ uint32(4129)
            else:
              ans <<= uint32(1)
        return uint16(ans)

    @staticmethod
    def _PSDES(lword, irword):
        assert(type(lword) is uint32 and type(irword) is uint32)
        c1 = [uint32(x) for x in [0xcba4e531, 0x537158eb, 0x145cdc3c, 0x0d3fdeb2]]
        c2 = [uint32(x) for x in [0x12be4590, 0xab54ce58, 0x6954c7a6, 0x15a2ca46]]
        for index in range(4):
            iswap = irword
            ia = irword ^ c1[index]
            itmpl = ia & uint32(0xffff)
            itmph = ia >> uint32(16)
            ib = (itmpl * itmpl) + ~(itmph*itmph)
            ia = (ib >> uint32(16)) | ((ib & uint32(0xffff)) << uint32(16))
            irword = (lword) ^ ((ia ^ c2[index]) + (itmpl * itmph))
            lword = iswap
        return lword, irword

    @staticmethod
    def get_checksum(buf, key):
        buf = np.array([buf], dtype='S').view('u1')
        crc = uint16(0)
        for i in range(len(buf)):
            crc = MauiSocket._DoCRC(crc, buf[i])
        lword, irword = MauiSocket._PSDES(uint32(crc), uint32(key))
        return "{:08x}{:08x}".format(lword, irword)

    @staticmethod
    def get_header(command, auth, key):
        msg = "TS={} AUTH={} DT={}".format(int(time.time()), auth, command)
        checksum = MauiSocket.get_checksum(msg, key)
        request = "CK={} ".format(checksum) + msg
        return "{:08d}\n{}".format(len(request), request)


    def __init__(self, authname):
        self.host = '129.32.84.162'
        self.port = 42559
        self.key = 0
        self.authname = authname

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self):
    #def __exit__(self, exc_type, exc_value, traceback):
        self.socket.close()

    def connect(self):
        #self.socket = socket.create_connection((self.host, self.port))
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        self.socket.setblocking(1)
        print self.socket.gettimeout()

    def checkjob(self, jobid):
        kws = (self.authname, jobid)
        command = "CMD=checkjob AUTH={} ARG=1 {} 0 \n".format(*kws)
        header = MauiSocket.get_header(command, self.authname, self.key)

        self.socket.sendall(header)
        self.socket.sendall(header)
        self.socket.sendall(header)
        self.socket.sendall(header)
        data = self.socket.recv(1024).strip()
        #res = self.socket.recv(9)
        #print res
        #size = int(res)
        #data = self.socket.recv(size)

        return data

class Model(object):
    def __init__(self, **kwargs):
        self.columns = []
        self.frame = pd.DataFrame()


        for k, v in kwargs:
            self.k = v

        self.maui = MauiSocket(getpass.getuser())
        self.maui.connect()

    def exit(self):
        self.maui.__exit__()

    def enqueue(self, event):
        self.event_queue.append(event)

    def handle_events(self):
        pass

    def get_job_id(self, xml_job):
        j = xml_job.find('Job_Id').text.split('.')[0]
        return j

    def parse_job(self, xml_job, jobid=None):
        job = {}
        update = False
        if jobid:
            job = {'id': jobid}
            update = True

        for child in iter(xml_job):
            tag, text = child.tag.lower(), child.text
            #if update and tag not in update_args: continue

            if tag == 'job_id':
                job['id'] = text.split('.')[0]

            elif tag == 'job_owner':
                text = text.split('@')[0]
                userinfo = pwd.getpwnam(text)
                fn = userinfo.pw_gecos
                if len(fn) > 17:
                    fn_items = fn.split()
                    fn_items = [i[0].upper() + '.' for i in fn_items[:-1]]+\
                                [fn_items[-1]]
                    job['job_owner_full'] = ' '.join(fn_items)
                else:
                    job['job_owner_full'] = fn

            # parse requested resources
            elif tag == 'exec_host':
                cpus = text.split('+')
                cpus = [cpu.split('/')[0] for cpu in cpus]
                job['cpunames'] = ', '.join(['%s[x%d]'%a for a in set((i, cpus.count(i)) for i in cpus)])
                job['nodenames'] = ', '.join(set(cpus))
                job['cpuct'] = str(len(cpus))

            # get priority if it exists (usually only for queued or held jobs)
            #elif 'priority' in tag:
            #    print job['id']
            #    res = self.maui.checkjob(job['id'])
            #    match = re.search(r'StartPriority:\s+(-?\d+)', res)
            #    if match:
            #        text = match.group(1)
            #        print text
            #    else:
            #        text = '----'

            #elif 'job_name' in tag:
            #    if len(text) > 13:
            #        if not self.trunc:
            #            text = '...'.join((text[:5], text[-5:]))

            elif tag in ('resources_used', 'resource_list'):
                for subchild in list(child):
                    stag, stext = subchild.tag, subchild.text
                    if stag == 'walltime':
                        stext = ':'.join(stext.split(':')[:-1])
                    job['.'.join((tag, stag))] = stext

            if tag not in allowed_args: continue
            job[tag] = text
        job['updated'] = 0
        return job

    def init_frame(self):
        data = check_output(['qstat', '-f', '-x'])
        root = ElementTree.fromstring(data)
        self.frame = pd.DataFrame(map(self.parse_job, root.findall("Job")))

    def update(self):
        # only things that will update are
        # priority, walltime, qtime, start_time, cputime
        # or the job is new/finished
        data = check_output(['qstat', '-f', '-x'])
        root = ElementTree.fromstring(data)

        self.frame.updated = np.zeros(len(self.frame))
        for xml_job in root.findall("Job"):
            jid = self.get_job_id(xml_job)

            if not jid in self.frame.id:
                new_job = self.parse_job(xml_job)
                self.frame = pd.concat((self.frame, pd.DataFrame([new_job])))
            else:
                for k, v in self.parse_job(xml_job, jobid=jid):
                    self.frame.loc[jid, k] = v
            self.frame.set_value(self.frame.id == jid, 'updated', 1)

        self.frame = self.frame[self.frame.updated != 0]

class Console(object):
    def __init__(self):
        self.model = Model()

    def exit(self):
        self.running = False
        self.model.exit()
        sys.exit(0)

    def get_input(self):
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)
        return ch

    def handle_keypress(self, kp):
        if kp in ('q', '\x03', '\x04', '\x1a'):
            self.exit()
        elif kp in ('\n', ' ', ''):
            pass

    def run(self, refresh=5):
        self.model.init_frame()
        self.event_queue = []

        self.running = True
        self.last_update = -1#time.time()

        while self.running:
            res = check_output(['cls' if os.name == 'nt' else 'clear'])

            if self.event_queue:
                self.handle_keypress(self.event_queue.pop(0))
            #try:
            #    self.model.handle_events(self.event_queue.pop(0))
            #except IndexError:
            #    pass
            if time.time() - self.last_update > refresh:
                self.model.update()
                self.last_update = time.time()

            self.display()
            self.event_queue.append(self.get_input())

    def display(self):
        print term
        print self.model.frame.head(3)
        #print self.model.frame.columns


def main():
    args = parse_arguments()

    if args.colorize:
        try:
            from colorama import Fore, Back, Style
        except:
            print ("\n\tFailed to load colorama package. Will not colorize "
                   "output\n\tIf you want to use the colorama package, you can "
                   "install it yourself or do\n\t\tmodule use "
                   "/home/tuf31071/privatemodules\n\t\tmodule load "
                   "conda-pyopencl\n")
            Fore, Back, Style = Empty(), Empty(), Empty()

    # run qstat -f with xml output and parse
    qstat_res = check_output(['qstat', '-f', '-x'])
    root = ElementTree.fromstring(qstat_res)

    job_list = []
    for xml_job in root.findall("Job"):
        job = {}
        for child in list(xml_job):
            tag, text = child.tag.lower(), child.text

            if tag in exclude_args: continue

            elif tag == 'job_id':
                job['id'] = text.split('.')[0]

            # get full details of job's owner
            elif tag == 'job_owner':
                text = text.split('@')[0]
                userinfo = pwd.getpwnam(text)
                fn = userinfo.pw_gecos
                if len(fn) > 17:
                    fn_items = fn.split()
                    fn_items = [i[0].upper() + '.' for i in fn_items[:-1]] +\
                                [fn_items[-1]]
                    job['job_owner_full'] = ' '.join(fn_items)
                else:
                    job['job_owner_full'] = fn
                job['job_owner_group'] = grp.getgrgid(userinfo.pw_gid).gr_name

            # parse requested resources
            elif tag == 'exec_host':
                cpus = text.split('+')
                cpus = [cpu.split('/')[0] for cpu in cpus]
                job['cpunames'] = ', '.join(['%s[x%d]'%a for a in set((i, cpus.count(i)) for i in cpus)])
                job['nodenames'] = ', '.join(set(cpus))
                job['cpuct'] = str(len(cpus))

            # get priority if it exists (usually only for queued or held jobs)
            elif args.priority == 'all' and 'priority' in tag:
                check_res = check_output(['checkjob', job['id']])
                match = re.search(r'StartPriority:\s+(-?\d+)', check_res)
                if match:
                    text = match.group(1)
                else:
                    text = '----'

            elif 'job_name' in tag:
                if len(text) > 13:
                    if not args.trunc:
                        text = '...'.join((text[:5], text[-5:]))

            elif tag in ('resources_used', 'resource_list'):
                for subchild in list(child):
                    stag, stext = subchild.tag, subchild.text
                    if stag == 'walltime':
                        stext = ':'.join(stext.split(':')[:-1])
                    job['.'.join((tag, stag))] = stext

            job[tag] = text

        job_list.append(job)


    if args.priority == 'idle':
        prio_res = check_output(['showq', '-i']).split('\n')
        prio_res = filter(None, map(str.strip, prio_res))
        prios = {}
        for line in prio_res[1:-1]:
            items = line.split()
            prios[items[0].strip('*')] = items[1]
        for job in job_list:
            if job['id'] in prios.keys():
                job['priority'] = prios[job['id']]
            else:
                job['priority'] = '----'

    # how to sort jobs. if full priority (meaning running jobs too) is
    # requested, sort by priority.  otherwise, unless different sorting is
    # requested, sort by state, owner, then id.
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    default_sort_args = ['state', 'queue', 'group', 'fname', 'id']
    sort_map = {'group': 'job_owner_group', 'uname': 'job_owner',
                'fname': 'job_owner_full', 'id': 'id', 'state': 'job_state',
                'queue': 'queue'}
    if args.priority:
        key = lambda d: (d['job_state'],
            -1*(int(d['priority']) if not d['priority'].startswith('--') else 50000),
            d['job_owner'], int(d['id']))
    else:
        sort_args = args.sort + [el for el in default_sort_args if el not in args.sort]
        key = lambda d: [convert(d[sort_map[el]]) for el in sort_args]

    job_list = sorted(job_list, key=key)

    # specify what args a printed.  this only changes if supplied a --fmt
    # command line option
    default_args = ['id', 'job_owner_full', 'job_owner_group', 'job_name',
            'resource_list.nodect', 'queue', 'resources_used.walltime',
            'resource_list.walltime', 'job_state']
    if args.priority:
        default_args += ['priority']
    if args.fmt:
        default_args = args.fmt

    # print and stylize the header
    fmt = ' '.join([allowed_args[k][0] for k in default_args])
    header = fmt.format(*[allowed_args[k][1] for k in default_args])
    if args.colorize:
        header = Style.BRIGHT + Fore.WHITE + header + Style.RESET_ALL
    print header

    dashfmt = fmt.replace(':<',':-<').replace(':^',':-^').replace(':>',':->')
    dashes = dashfmt.format(*['']*len(default_args))
    print dashes

    # handle squashing
    if args.squash:
        user_squash = []
        group_squash = []
        # TODO: do this


    if args.colorize:
        styles = [Style.DIM, Style.NORMAL]
        stylei = 0

    totals = {'gpu':0, 'normal':0}

    for job in (job_list if not args.reverse else reversed(job_list)):
        totals[job['queue']] += int(job['resource_list.nodect']) \
                if job['job_state'] == 'R' else 0

        if args.squash:
            pass

        if args.user:
            if job['job_owner'] != args.user: continue
        elif args.me:
            if job['job_owner'] != getpass.getuser(): continue

        if args.queue:
            if job['queue'] != args.queue: continue

        if args.running:
            if job['job_state'] != 'R': continue

        if args.queued:
            if job['job_state'] != 'Q': continue

        line = fmt.format(*[job.get(k, '-') for k in default_args])
        if args.colorize:
            if job['job_owner'] == getpass.getuser():
                if job['job_state'] == 'R':
                    line = Fore.YELLOW + Style.BRIGHT + line + Style.RESET_ALL
                else:
                    line = Fore.MAGENTA + line + Style.RESET_ALL
            elif args.running or args.queued:
                line = styles[(stylei/5)%2] + line
                stylei += 1
            else:
                style = Style.NORMAL if job['job_state'] == 'R' else Style.DIM
                line = style + line

        print line

    print dashes
    print header
    print

    total = "\tNormal Q:   {normal}/60\t||\tGPU Q:   {gpu}/10".format(**totals)
    if args.colorize:
        print Style.BRIGHT + Fore.WHITE + total + Style.RESET_ALL
    else:
        print total
    print


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-u', '--user', type=str,
                        help="Only show results for [user]")
    parser.add_argument('-U', '--me', action='store_true',
                        help="Only show results for [you]")
    parser.add_argument('-R', '--running', action='store_true',
                        help="Hide non-running jobs")
    parser.add_argument('-Q', '--queued', action='store_true',
                        help="Hide running jobs")
    parser.add_argument('-q', '--queue', type=str, choices=['gpu', 'normal'],
                        help="Only show results for [queue]")
    parser.add_argument('-t', '--trunc', action='store_true',
                        help="Jobnames are shown as first & last 5 characters "
                             "by default. This will show first 13 characters")
    parser.add_argument('-r', '--reverse', action='store_true',
                        help=("Reverses the displayed output.  Works with "
                              "other flags"))
    parser.add_argument('-p', '--priority', nargs='?',
                        const='idle', choices=['idle', 'all'],
                        help="Show job priorities. 'idle' (default) queuries `showq` "
                             "and will only output priority for idle jobs. "
                             "All queries checkjob and shows priority for "
                             "all jobs, but that takes about 15 seconds")
    parser.add_argument('-c', '--colorize', action='store_false',
                        help="REMOVE color output (if your terminal had supported it). "
                             "This requires an external package which I have "
                             "in my conda-pyopencl module which you can "
                             "module load with"
                             "`module use /home/tuf31071/privatemodules` "
                             "`module load conda-pyopencl`"
                             " (ON BY DEFAULT)")
    #parser.add_argument('-s', '--sort', type=str, metavar='',
    #                    help="Keys by which to sort results. Can supply more "
    #                         "than one using a comma (,) separated list")
    parser.add_argument('-s', '--sort', nargs='+', default=[],
                        choices=['group', 'uname', 'fname', 'id', 'queue'],
                        help=("Additional sorting options.  Can be combined in "
                              "a space separated list"))
    parser.add_argument('-S', '--squash', choices=['user', 'group'],
                        help="Show user/group aggregated output")
    parser.add_argument('--clear', action='store_true',
                        help="Clear previous terminal output before printing")
    parser.add_argument('-f', '--fmt', nargs='+', metavar='',
                        help="Custom formatting. Give space separated list of "
                             "options: " + ', '.join(sorted(allowed_args.keys())))


    args = parser.parse_args()

    if args.fmt:
        if args.squash:
            raise Exception("Can't squash with custom formatting")
        for item in args.fmt:
            if item not in allowed_args.keys():
                raise Exception("Invalid option keyword: %s", item)

    if args.priority == 'all':
        print "\n\tQuerying for job priorities. Will take a minute...\r",
        sys.stdout.flush()
    elif args.clear:
        print 'ran'
        res = check_output(['cls' if os.name == 'nt' else 'clear'])
        print res
    else:
        print

    return args

if __name__ == "__main__":
    Console().run()
