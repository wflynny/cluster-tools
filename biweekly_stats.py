#!/usr/bin/env python
import os
import sys
import pwd
import datetime as dt

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

STATS_DIR = '/opt/maui/stats'
FS_TARGETS = {'cbcb': 20.00,
              'icms': 20.00,
              'hpc': '-----',
              'hey': 14.00,
              'roder_voelz': 14.00,
              'dunbrack': 18.00,
              'schafmeister': 14.00}

def gather_files(start_file, end_file):
    start_mtime = os.stat(start_file).st_mtime
    end_mtime = os.stat(end_file).st_mtime

    files = []
    for filename in os.listdir(STATS_DIR):
        if filename.startswith('FS'): continue
        filename = os.path.join(STATS_DIR, filename)

        mtime = os.stat(filename).st_mtime
        if start_mtime < mtime < end_mtime:
            files.append((mtime, filename))

    files = [f for t, f in sorted(files)]

    return [start_file] + files + [end_file]

def parse_statsfiles(files):
    users = {}
    groups = {}
    total = {'jobs': 0, 'proc': 0}
    for filename in files:
        if not os.path.exists(filename): continue
        with open(filename, 'r') as fin:
            fin.next()
            for line in fin:
                if line.startswith('#'): continue
                items = line.split()
                if len(items) < 10: continue
                keep = []

                # job isn't completed
                if items[6] != "Completed": continue

                uid = items[3]
                guid = items[4]
                queue = items[7][1:-1].split(':')[0]

                st_time = int(items[10])
                end_time = int(items[11])
                qd_time = int(items[20])

                nodes = int(items[21])
                #procs = int(items[2])
                #usage = float(end_time - st_time) * nodes * procs
                usage = float(items[29])
                if usage > (end_time - st_time) * nodes:
                    usage /= nodes

                if uid not in users:
                    users[uid] = {'jobs': 0, 'proc': 0, 'qtime': 0,
                                         'group': guid}
                users[uid]['jobs'] += 1
                users[uid]['proc'] += usage
                users[uid]['qtime'] += st_time - qd_time

                if guid not in groups:
                    groups[guid] = {'jobs': 0, 'proc': 0, 'qtime': 0}
                groups[guid]['jobs'] += 1
                groups[guid]['proc'] += usage
                groups[guid]['qtime'] += st_time - qd_time

                total['jobs'] += 1
                total['proc'] += usage

    return users, groups, total

def format_stats(files):
    users, groups, totals = parse_statsfiles(files)

    grp_fmt_h = ('{name:<12}  {fs:>10}  {jobs:>6}  {jobs_pc:>8}'
                 '  {pc_hours:>12}  {qtime:>6}')
    grp_fmt = ('{name:<12}  {fs:>10}  {jobs:>6}  {jobs_pc:>8.2f}'
               '  {pc_hours:>12.2f}  {qtime:>6.2f}')
    header_items = dict(name="Group", fs="FS Target", jobs="Jobs",
                        jobs_pc="Job %", pc_hours="Proc-Hours %",
                        qtime="QTime")
    grp_header = grp_fmt_h.format(**header_items)

    user_fmt_h = ('{name:<8}  {fullname:<20.20}  {grp:<12}  {jobs:>6}'
                  '  {jobs_pc:>8}  {pc_hours:>12}  {qtime:>6}')
    user_fmt = ('{name:<8}  {fullname:<20.20}  {grp:<12}  {jobs:>6}'
                '  {jobs_pc:>8.2f}  {pc_hours:>12.2f}  {qtime:>6.2f}')
    header_items['name'] = "User"
    header_items['fullname'] = "FullName"
    header_items['grp'] = "Group"
    user_header = user_fmt_h.format(**header_items)

    group_stats = []
    for group, values in sorted(groups.iteritems()):
        out = {}
        out['name'] = group
        out['fs'] = FS_TARGETS[group]
        out['jobs'] = values['jobs']
        out['jobs_pc'] = values['jobs']/float(totals['jobs'])*100
        out['pc_hours'] = values['proc']/totals['proc']*100
        out['qtime'] = values['qtime'] / float(values['jobs']) / 3600.
        group_stats.append(grp_fmt.format(**out))
    group_stats = [grp_header] + group_stats

    user_stats = []
    for user, values in sorted(users.iteritems(), key=lambda t: (t[1]['group'], t[0])):
        out = {}
        out['name'] = user
        out['fullname'] = pwd.getpwnam(user).pw_gecos
        out['grp'] = values['group']
        out['jobs'] = values['jobs']
        out['jobs_pc'] = values['jobs']/float(totals['jobs'])*100
        out['pc_hours'] = values['proc']/totals['proc']*100
        out['qtime'] = values['qtime'] / float(values['jobs']) / 3600.
        user_stats.append(user_fmt.format(**out))
    user_stats = [user_header] + user_stats

    return group_stats, user_stats

def send_mail(start, end, group_stats, user_stats):
    start = start.strftime('%B %d %Y')
    end = end.strftime('%B %d %Y')

    key = (("FS Target", "Target Fairshare percentage"),
           ("Jobs", "Number of jobs"),
           ("Jobs %", "Percentage of all jobs"),
           ("Proc-Hours %", "Percentage of all CPU time utilized"),
           ("QTime", "Average QTime in hours"))

    title = "\nCB2RR biweekly usage summary for %s - %s"%(start, end)

    body = title
    body += "\n\nGroup Usage:\n" + '\n'.join(group_stats)
    body += "\n\nUser Usage:\n" + '\n'.join(user_stats)
    body += "\n\nKey:\n" + '\n'.join(['{0:<12} - {1}'.format(*k) for k in key])
    print body
    sys.exit()

    html = '<html><head></head><body>'
    html += '<font face="Courier New, Courier, monospace">'
    html += body.replace(' ', '&nbsp;').replace('\n', '\n<br><br>') + '</font>'
    html += '</body></html>'
    html = html.replace("Group&nbsp;Usage:", "<b>Group&nbsp;Usage:</b>")
    html = html.replace("User&nbsp;Usage:", "<b>User&nbsp;Usage:</b>")

    fromaddr = 'acct@cb2rr.cst.temple.edu'
    toaddr = 'tuf31071@temple.edu'
    subject = 'Biweekly Usage Statistics for CB2RR'

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = fromaddr

    recipients = ['tuf31071@temple.edu', 'tuf29141@temple.edu',
                  'ronlevy@temple.edu']
    # TODO: COMMENT FOLLOWING LINE TO SEND TO ALL RECIPIENTS
    recipients = ['tuf31071@temple.edu']
    msg['To'] = ", ".join(recipients)
    msg.attach(MIMEText(body, 'plain'))
    msg.attach(MIMEText(html, 'html'))

    server = smtplib.SMTP('localhost')
    server.ehlo()
    server.sendmail(fromaddr, recipients, msg.as_string())
    server.close()

def main():
    end = dt.datetime.now()
    start = end - dt.timedelta(days=14)

    start_file = os.path.join(STATS_DIR, start.strftime('%a_%b_%d_%Y'))
    end_file = os.path.join(STATS_DIR, end.strftime('%a_%b_%d_%Y'))

    files = gather_files(start_file, end_file)
    group_stats, user_stats = format_stats(files)

    send_mail(start, end, group_stats, user_stats)


if __name__ == "__main__":
    main()
