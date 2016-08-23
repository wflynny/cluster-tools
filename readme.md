# Cluster Tools

A various collection of scripts that interface with torque/maui on various
high performance computing clusters I've used in the past.  Helps streamline
information from tools such as `qstat`, `showbf`, `showfree`, etc.


### bqstat.py

B(etter) qstat shows more user centric information and uses other maui/torque,
default linux, and default python libraries to refine the default information 
displayed by qstat.  Examples of better information:

-   Colorized output
-   Fast queued/held job priorities, all job priorities available but takes a
    few seconds to fetch
-   Full user names for when you're wondering who is using half the cluster
-   Better job sorting, options to make the user stand-out
-   Summary stats
-   More

### who.py

Envisioned as a login script to show you who is using the cluster at login.
Useful for seeing who is on in case you want to abuse the head node or want
to bug anyone with tty `write`s (using `message.py`.

### biweekly_stats.py

Some useful parsing of maui log files over the past two weeks that gets
formatted for email.  The majority of the script is pretty hacky since the
maui documentation sucks and the smtp server on localhost for our newer
cluster let's you spoof an email from anyone without authentication (don't
tell anyone).  The only hard-coded things are the specific fairshare targets
for our cluster's groups and the email related things.

### cumulative_stats.py

See [biweekly_stats.py](#biweekly_stats.py) but replace "the past two weeks" 
with "for all time".

### queuejobs.sh/queuetime.sh

Custom graph scripts for Ganglia that work _ok_.
