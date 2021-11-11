Python iterface for reading and appending tar files, while keeping a
fast index for finding and reading files in the archive.

This interface has been successfully used to store and manage over one
billion files in one of the Pilot-2 simulation campaigns.

Written by Tomas Oppelstrup as part of the DOE/NCI Pilot-2 project,
based on an initial (simpler) index implementation by Tomas
Oppelstrup, December 11, 2018. Updated to support python 3 and to
become a module by Francesco DiNatale.

A script for correctness checking and examples of use can be found in
tests/indextest.py. The simplest way to run the tests is by e.g.:
   cd test
   ./testscript.sh
