#!/bin/bash
#
# Simple script to generate ganglia
# metric measuring minimum wait time for
# an eligible but idle job to start.
#

QJOBS=`/opt/maui/bin/showq -i|tail -1|perl -pe 's/Jobs: ([\d]+) *Total.*/$1/'`
/opt/ganglia/bin/gmetric -u "Jobs" -t float -v $QJOBS -n "Queued Jobs"
