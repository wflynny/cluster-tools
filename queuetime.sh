#!/bin/bash
#
# Simple script to generate ganglia
# metric measuring minimum wait time for
# an eligible but idle job to start. 
#

MINQTIME=`/opt/maui/bin/showq -i|tail -1|perl -pe 's/.*ProcHours *\(([\d.]+) *Hours\)/$1/'`
/opt/ganglia/bin/gmetric -u "Hours" -t float -v $MINQTIME -n "Min Queuetime"
