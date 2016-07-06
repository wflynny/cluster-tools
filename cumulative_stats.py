#!/usr/bin/env python

import os
import sys
import pwd
import grp
import datetime

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from subprocess import check_output

def parse_user_group_stats(user=False):
    cmd = ['showstats']
    cmd += ['-u'] if user else ['-g']
    res = check_output(cmd).split('\n')

    grp_fmt = ('{name:<12}  {fs:>10}  {jobs:>6}  {jobs_pc:>8}  {pc_hours:>12}'
               '  {qtime:>6}')
    header_items = dict(name="Group", fs="FS Target", jobs="Jobs",
                        jobs_pc="Job %", pc_hours="Proc-Hours %",
                        qtime="QTime")
    grp_header = grp_fmt.format(**header_items)

    user_fmt = '{name:<8}  {fullname:<20.20}  {grp:<12}  {jobs:>6}  {jobs_pc:>8}  {pc_hours:>12}  {qtime:>6}'
    header_items['name'] = "User"
    header_items['fullname'] = "FullName"
    header_items['grp'] = "Group"
    user_header = user_fmt.format(**header_items)

    stats = []
    for line in filter(None, res)[3:]:
        items = line.split()
        out = {}
        out['name'] = items[0]

        if user:
            res2 = pwd.getpwnam(out['name'])
            out['fullname'] = res2.pw_gecos
            out['grp'] = grp.getgrgid(res2.pw_gid).gr_name

        out['jobs'] = items[4]
        out['jobs_pc'] = items[5]
        out['pc_hours'] = items[9]

        out['fs'] = items[10]
        out['qtime'] = items[13]

        if user: stats.append(user_fmt.format(**out))
        else: stats.append(grp_fmt.format(**out))

    return [user_header if user else grp_header] + \
           sorted(stats)

def parse_summary_stats():
    res = check_output(['showstats', '-s']).split('\n')

    stats = []
    for line in filter(None, res)[3:]:
        items = line.strip().split(':')
        items = map(str.strip, items)
        # crappy regex replacement to remove excess white space
        stats.append((items[0], items[1].replace(' ', '').replace('(', ' (')))

    fmt = '{:<30}   {:<24}'
    stats = [fmt.format(*s) for s in stats]

    return stats


def send_mail():
    group_stats = parse_user_group_stats()
    user_stats = parse_user_group_stats(user=True)
    summary_stats = parse_summary_stats()

    now = datetime.datetime.now()
    #now -= datetime.timedelta(days=28)
    start = datetime.datetime.strptime('Dec 31 2015', '%b %d %Y')

    body = "\nCumulative <CLUSTER NAME> usage summary from %s to %s"%(start.strftime('%b %d %Y'), now.strftime('%b %d %Y'))

    body += "\n\nSummary:\n" + '\n'.join(summary_stats)
    body += "\n\nGroup Usage:\n" + '\n'.join(group_stats)
    body += "\n\nUser Usage:\n" + '\n'.join(user_stats)

    html = '<html><head></head><body>'
    html += '<font face="Courier New, Courier, monospace">'
    html += body.replace('\n', '\n<br><br>').replace(' ', '&nbsp;') + '</font>'
    html += '</body></html>'

    #TODO: add 'from' address
    fromaddr = 'acct@your.cluster.com'
    subject = 'Cumulative Usage Statistics for <CLUSTER NAME HERE>'

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = fromaddr

    recipients = ['you@yours.com']
    msg.attach(MIMEText(body, 'plain'))
    msg.attach(MIMEText(html, 'html'))

    server = smtplib.SMTP('localhost')
    server.ehlo()
    server.sendmail(fromaddr, recipients, msg.as_string())
    server.close()

def reset_stats():
    res = check_output(['resetstats'])

def main():
    send_mail()



if __name__ == "__main__":
    main()
