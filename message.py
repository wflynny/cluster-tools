#!/usr/bin/env python
import os
import sys
import pwd
import argparse

from subprocess import check_output, Popen, STDOUT, PIPE

def quit(msg):
    print msg
    sys.exit(1)

def resolve_user(user):
    tuid = None
    if user.startswith('tu'):
        tuid = user
    else:
        content = check_output(['finger', user]).strip()
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

        name, stat, tty = line[0], line[1], line[2]
        if stat == '-':
            print "Warning: found user: {}  at tty: {}, but terminal mesg = 'n'".format(name, tty)
        if given_tty and tty != given_tty: continue
        ttys.append(tty)
    return ttys

def message(args):
    tuid = resolve_user(args.user)
    ttys = get_ttys(tuid, args.tty)
    if not ttys:
        quit("No valid ttys found for user: {}".format(tuid))

    if not args.all:
        ttys = [ttys[0]]

    procs = []
    for tty in ttys:
        writer = Popen(['write', tuid, tty], stdin=PIPE, stdout=PIPE)
        procs.append(writer)

    try:
        while True:
            input = raw_input("> ")
            for writer in procs:
                writer.stdin.write(input + '\n')
            #for writer in procs:
            #    print "< " + repr(writer.stdout.read())
    except (EOFError, KeyboardInterrupt) as e:
        print
        for proc in procs:
            proc.stdin.close()
            proc.wait()




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('user', type=str, help="User to message")
    parser.add_argument('-a', '--all', action='store_true',
                        help="Message user at all active termainls")
    parser.add_argument('-t', '--tty', type=str, default=None,
                        help="tty through which to write to user")

    args = parser.parse_args()
    message(args)
