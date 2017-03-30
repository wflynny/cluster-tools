#!/usr/bin/env python
from __future__ import unicode_literals
import re
import pwd, grp
import argparse
from datetime import datetime as dt
from colorama import Fore, Back, Style
from subprocess import check_output

E1, E2, E3, E4 = u'\u2581'*3,  u'\u2582'*3,  u'\u2583'*3,  u'\u2584'*3
E5, E6, E7, E8 = u'\u2585'*3,  u'\u2586'*3,  u'\u2587'*3,  u'\u2588'*3
ES = [u'___', E1, E2, E3, E4, E5, E6, E7, E8]

def weight_func(x):
    if x < 0.001:
        return u'   '
    return ES[int(round(x/12.5))]

def weight_map(weights, target=None, target_index=None):
    if isinstance(weights[0], basestring):
        weights = map(float, weights)

    if target:
        colors = [Fore.RED if float(w) > float(target) else Fore.GREEN
                  for w in weights]
        if target_index:
            colors[target_index] = Fore.WHITE
    weights = map(weight_func, weights)
    #weights = map(lambda x: ES[int(round(x/12.5))], weights)
    if target:
        weights = [''.join(tup) for tup in zip([u'']*len(weights), colors, weights,
                                [Style.RESET_ALL]*len(weights))]
    return weights

def daystrings():
    week = 'MTWTFSS'*3
    x = dt.today().isoweekday()
    return map(''.join, zip(map(str, range(14)), week[x:x+14][::-1]))

def users(lines, weights, args):
    print
    fmt = u'{:>8.8}|{:>8.8}|{:^3}|' + '{:^3}|'*14
    print fmt.format('','','', *daystrings())
    print fmt.format('User', 'Group', 'FS', *weight_map(weights))
    print fmt.format('-'*12, '-'*8, *['-'*3]*15)

    users = []
    for line in lines:
        u = line[0].strip('*- ')
        if u == 'DEFAULT': continue
        info = pwd.getpwnam(u)
        g = grp.getgrgid(info.pw_gid).gr_name

        us = line[1]
        target = float(line[2])

        w = [c if not c.startswith('-') else '0.' for c in line[3:]]
        w = weight_map([us] + w, target=target)
        users.append((g, fmt.format(g, u, *w)))

    for _, user in sorted(users):
        print user

def groups(lines, weights, args):
    print
    fmt = u'      {:>8.8}|{:<3}{:>3}|' + '{:^3}|'*14
    print fmt.format('','','', *daystrings())
    print fmt.format('Group', 'FS', 'TGT', *weight_map(weights))
    print fmt.format(' '*3+'-'*5, *['-'*3]*16)

    groups = []
    for line in lines:
        g = line[0].strip('*- ')
        if g == 'DEFAULT': continue
        if g == 'ACCT': break

        w = [c.strip('-') if not c.startswith('-') else '0.' for c in line[1:]]
        w = weight_map(w, target=w[1], target_index=1)
        groups.append((g, fmt.format(g, *w)))

    for _, group in sorted(groups):
        print group

def main(args):
    res = check_output(['diagnose', '-f'])

    lines = re.split('\n+', res)
    lines = [re.split('\s+', line) for line in lines if not line.startswith('-')]
    lines = filter(None, lines)[5:]

    weights = [w*100 for w in map(float, lines[0][3:])]
    user_idx = lines.index(['USER'])
    group_idx = lines.index(['GROUP'])
    acct_idx = lines.index(['QOS'])
    user_lines = lines[user_idx+1:group_idx]
    group_lines = lines[group_idx+1:acct_idx]

    if args.users or (not args.users and not args.groups):
        users(user_lines, weights, args)
    if args.groups or (not args.users and not args.groups):
        groups(group_lines, weights, args)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--users', action='store_true')
    parser.add_argument('-g', '--groups', action='store_true')
    main(parser.parse_args())
