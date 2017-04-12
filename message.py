#!/usr/bin/env python2
import os
import sys
import pwd
import smtplib
import argparse
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from subprocess import check_output, Popen, STDOUT, PIPE

def quit(msg):
    print msg
    sys.exit(1)

def resolve_user(user, debug=False):
    tuid = None
    if user.startswith('tu'):
        tuid = user
    else:
        if len(user.split(' ')) > 1:
            cmd = ['finger'] + user.split(' ')
        else:
            cmd = ['finger', user]
        content = check_output(cmd).strip()
        lines = content.split('\n')
        if '' in lines:
            quit("Full name search for: {} yields multiple users".format(user))
        tuid = lines[0].split()[1]
        print "Resolved name: {}  to user: {}".format(user, tuid)

    if not tuid:
        quit("Username or full name: {} does not resolve on this machine".format(user))

    try:
        x = pwd.getpwnam(tuid)
    except:
        quit("TUid: {} does not exist on this machine".format(user))
    return tuid


def get_ttys(user, given_tty=None):
    content = check_output(['who', '-T']).strip()
    ttys = []
    messageable = True
    for line in content.split('\n'):
        if not line.startswith(user): continue
        line = line.split()

        name, stat, tty, date, time = line[:5]
        dt = datetime.strptime('T'.join((date, time)), '%Y-%m-%dT%H:%M')
        if stat == '-':
            print "Warning: found user: {}  at tty: {}, but terminal mesg = 'n'".format(name, tty)
        if given_tty and tty != given_tty: continue
        ttys.append((tty, dt))
    return ttys

def message(args):
    all_ttys = []
    tuids = []
    for user in args.user:
        tuid = resolve_user(user)
        ttys = get_ttys(tuid, args.tty)
        if not ttys:
            quit("No valid ttys found for user: {}".format(tuid))
        tuids.append(tuid)

        if not args.all:
            #choose most recently active
            now = datetime.now()
            ttys = [ttys[min([(now-t[1], i) for i, t in enumerate(ttys)])[1]][0]]
        else:
            ttys = [t[0] for t in ttys]
        all_ttys.extend(ttys)

    procs = []
    for tty in all_ttys:
        writer = Popen(['write', tuid, tty], stdin=PIPE, stdout=PIPE)
        procs.append(writer)

    content = []
    try:
        if args.infile:
            content = args.infile.read()
            for writer in procs:
                writer.stdin.write(content)
        else:
            while True:
                input = raw_input("> ")
                content.append(input + '\n')
                for writer in procs:
                    writer.stdin.write(input + '\n')
            #for writer in procs:
            #    print "< " + repr(writer.stdout.read())
    except (EOFError, KeyboardInterrupt) as e:
        print
        for proc in procs:
            proc.stdin.close()
            proc.wait()

    if args.email:
        send_email(tuids, content)
    return

def send_email(tuids, content):
    if isinstance(content, list):
        content = ' '.join(content)
    if not isinstance(tuids, list):
        tuids = [tuids]
        assert isinstance(tuids[0], str)

    body = content + '\n\n'

    html = '<html><head></head><body>'
    html += body.replace(' ', '&nbsp;').replace('\n', '\n<br><br>')
    html += '</body></html>'

    # TODO: add 'from' address
    fromaddr = 'acct@cb2rr.cst.temple.edu'
    subject = 'CB2RR file system usage notice'

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = fromaddr

    # TODO: add receipients
    recipients = [tuid+'@temple.edu' for tuid in tuids]
    msg['To'] = ", ".join(recipients)
    msg.attach(MIMEText(body, 'plain'))
    msg.attach(MIMEText(html, 'html'))

    server = smtplib.SMTP('localhost')
    server.ehlo()
    server.sendmail(fromaddr, recipients, msg.as_string())
    server.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('user', type=str, nargs='+', help="User to message")
    parser.add_argument('-a', '--all', action='store_true',
                        help="Message user at all active termainls")
    parser.add_argument('-t', '--tty', type=str, default=None,
                        help="tty through which to write to user")
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="extra debugging info")
    parser.add_argument('-i', '--infile', type=argparse.FileType('r'),
                        help="Read message from file instead of input()")
    parser.add_argument('-e', '--email', action='store_true',
                        help="Also send email to <user>@temple.edu")

    args = parser.parse_args()
    message(args)
