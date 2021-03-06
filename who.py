#!/usr/bin/env python
import os
import sys
import re
import datetime
import argparse

from subprocess import check_output

def main(args):
    res = check_output(['finger'])
    users = {}
    lines = res.strip().split('\n')
    fmt = re.compile(r'^([a-z0-9]+)\s([A-z\.\s]+)(pts\/\d+).*([A-z]{3}\s+\d{1,2}\s\d{2}:\d{2})')
    for line in lines[1:]:
        match = fmt.search(line)
        if not match:
            continue
        user, fullname, pts, login = match.groups()
        info = users.get(user, None)
        if info:
            last_login = datetime.datetime.strptime(info[1], '%b %d %H:%M')
            new_login = datetime.datetime.strptime(login, '%b %d %H:%M')
            if new_login < last_login:
                login = info[1]
        nlogins = info[-1] + 1 if info else 1
        users[user] = [fullname.strip(), login, pts, nlogins]

    for user in users:
        res = check_output(['groups', user])
        left = res.split(':')[-1].strip()
        group = left.split(' ')[0]
        users[user].insert(0, group)

    fmt = '{0:10} {1[0]:<14} {1[1]:<30} {1[4]:<2}  {1[2]:<15} {1[3]}'

    print
    print "Currently logged in users" + ("" if not args.group else " in group: "+args.group)
    print
    print fmt.format('user', ['group','fullname','lastlogin','tty','N'])
    for user, vals in sorted(users.iteritems(), key=lambda t: t[1][0]):
        if args.group and vals[0] != args.group: continue
        print fmt.format(user, vals)
    print
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--group',
                        help="Only show users from <group>")
    main(parser.parse_args())
