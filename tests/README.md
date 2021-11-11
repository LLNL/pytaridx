Test script for pytaridx
------------------------

There are two scripts: indextest.py and testscript.sh.

If you run indextest.py (with either python or python3) then you'll
get printout of some timings and some consistency checks. If you get
no exceptions/crashes, and all lines that say "failcount" has
failcount = 0, then all tests are ok. The tests include creating an
indexed tar file, adding files, checking that all files in it are
there, and with correct content, and checking that finding the last
added member is correctly found.

The second script, testscript.sh, is a bash script that calls
indextest.py using both python and python3, and reports "OK" if no
problems are found, and "FAIL" otherwise. It checks that the python
execution succeeded, that the failcount reports 0 (zero), and that the
listing of tarfile members (newfile.tar.pylst) has the same members as
standard tar thinks (e.g.  tar tf newfile.tar). You can add the "-v"
command line switch, which gives verbose output, meaning that you get
the all the standard output from the python execution. The bash script
removes the created tar file and index files on startup and at the
end.

You can use the python test script (indextest.py) to see how pytaridx
works and as an example of the API.
