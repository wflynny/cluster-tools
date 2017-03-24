#!/usr/bin/env python
"""
Customizable qstat script because the default qstat sucks
"""
import re, os, sys
import pwd, grp
import getpass
from xml.etree import ElementTree
try:
    import argparse
    from subprocess import check_output
except:
    print "\n\tError:",
    print "\tThis script requires at least Python 2.7 for basic functionailty."
    print "\t\tUse one of the available python modules (anaconda or opt-python)"
    print "\t\tor use my conda-pyopencl module in /home/tuf31071/privatemodules\n"
    sys.exit(1)

class Empty(object):
    def __getattribute__(self, name):
        return ""

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
                'server':                   ['{:<8}',"Server"],
                'checkpoint':               ['{:<}',"Checkpoint"],
                'ctime':                    ['{:<}',"Current Time"],
                'error_path':               ['{:<}',"Error Path"],
                'exec_host':                ['{:<20.20}',"CPUs"],
                'cpunames':                 ['{:<14.14}',"CPUs"],
                'nodenames':                ['{:<7}',"Nodelist"],
                'exec_port':                ['{:<8}',"Port"],
                'gpuct':                    ['{:^4}',"GPUs"],
                'cpuct':                    ['{:^4}',"CPUs"],
                'exec_gpus':                ['{:<20.20}',"GPUs"],
                'mtime':                    ['{:<}',"Mtime"],
                'output_path':              ['{:<}',"Out Path"],
                'qtime':                    ['{:<10}',"Queue Time"],
                'rerunable':                ['{:<}',"Rerunable?"],
                'resource_list.nodect':     ['{:>3}',"# N"],
                'resource_list.nodes':      ['{:<}',"Node args"],
                'resource_list.walltime':   ['{:^6.6}',"RTime"],
                'session_id':               ['{:<6}',"SessID"],
                'euser':                    ['{:<10}',"Username"],
                'egroup':                   ['{:<12}',"User Group"],
                'queue_type':               ['{:<}',"Queue Type"],
                'comment':                  ['{:<}',"Comment"],
                'etime':                    ['{:<8.8}',"Elapsed Time"],
                'submit_args':              ['{:<}',"Submission Args"],
                'start_time':               ['{:<}',"Start Time"],
                'walltime.remaining':       ['{:<}',"Time Remaining"],
                'start_count':              ['{:<}',"Start Count"],
                'fault_tolerant':           ['{:<}',"Fault Tolerant?"],
                'job_radix':                ['{:<}',"Radix"],
                'submit_host':              ['{:<}',"Host"],
                'gpu_flags':                ['{:<12}',"Using GPUs?"]}


exclude_args = ('variable_list', 'pbs_o_logname', 'pbs_o_path', 'pbs_o_mail',
                'pbs_o_lang', 'pbs_o_workdir', 'pbs_o_host', 'hold_types',
                'join_path', 'keep_files', 'mail_points',)

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
    if not qstat_res:
        exit("Nothing currently running...\n")

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

    totals = dict((queue, 0) for queue in args.queues)#'gpu':0, 'normal':0}

    for job in (job_list if not args.reverse else reversed(job_list)):
        totals[job['queue']] += int(job['resource_list.nodect']) \
                if job['job_state'] == 'R' else 0

        if args.squash:
            pass

        if args.group:
            if job['job_owner_group'] != args.group: continue

        if args.user:
            if job['job_owner'] != args.user and \
               (args.user.lower() not in job['job_owner_full'].lower()): continue
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

    total = ""
    for queue, count in sorted(totals.iteritems(), key=lambda t: t[1]):
        total += "{queue:>10} Q:   {count}/{limit}\n".format(queue=queue.upper(),
                count=count, limit=args.limits[queue])
    #total = "\tNormal Q:   {normal}/60\t||\tGPU Q:   {gpu}/10".format(**totals)
    if args.colorize:
        print Style.BRIGHT + Fore.WHITE + total + Style.RESET_ALL
    else:
        print total

def load_qos(args):
    content = ''
    try:
        with open(args.maui_cfg, 'r') as fin:
            content += fin.read()
    except:
        #that failed, try check_output(showconfig)
        try:
            content += check_output(['showconfig'])
        except:
            raise Exception("Can't find config file to read queues")
    queues = re.findall(r'RESERVATIONQOSLIST\[\d\]\s+(\w+)qos', content)

    limits = {}
    try:
        content = check_output(['pbsload'])
        for queue in queues:
            limits[queue] = content.count('queue=%s'%queue)
    except:
        pass
    return queues, limits

def parse_arguments():
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument('--maui-cfg', default='/opt/maui/maui.cfg',
                        help=("Path to Maui config files.  If blank or can't "
                              "find, will use `showconfig` (slower)"))
    args_so_far, _ = parent.parse_known_args()
    queues, limits = load_qos(args_so_far)

    parser = argparse.ArgumentParser(description=__doc__, parents=[parent])
    parser.add_argument('-g', '--group', type=str,
                        help="Only show results for [group]")
    parser.add_argument('-u', '--user', type=str,
                        help="Only show results for [user]")
    parser.add_argument('-U', '--me', action='store_true',
                        help="Only show results for [you]")
    parser.add_argument('-R', '--running', action='store_true',
                        help="Hide non-running jobs")
    parser.add_argument('-Q', '--queued', action='store_true',
                        help="Hide running jobs")
    parser.add_argument('-q', '--queue', type=str, choices=queues,
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
    args.queues = queues
    args.limits = limits

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
    main()
