#!/bin/bash

# Written by Tomas Oppelstrup as part of the DOE/NCI Pilot-2 project.

# SPDX-License-Identifier: MIT

# Bash script that runs some correctness tests on the pytaridx
# module. Tests are performed both using python2 and pytthon 3.

if [ "x$1" = "x-v" ] ; then
    verbose=1
else
    verbose=0
fi

for v in 2 3 ; do
    echo "* PYTHON $v TEST"
    rm -f newfile.tar*

    suff=$v
    if [ v = "2" ] ; then suff="" ; fi

    if ( python$suff indextest.py > $v.out ) ; then
	if [ $verbose = 1 ] ; then cat $v.out ; fi
	if ( grep failcount $v.out | grep -v 'failcount = 0' ) ; then
	    echo "FAIL"
	else
	    if ( diff -q \
	   	   <(tar tf newfile.tar) \
		   <(sed -e 's/,.*$//' < newfile.tar.pylst) \
	       ) ; then
	    
		echo "OK"
	    
	    else	    
		echo "FAIL"
	    fi
	fi
    else
	if [ $verbose = 1 ] ; then cat $v.out ; fi
	echo "FAIL"
    fi
    rm $v.out
done

rm -f newfile.tar*
