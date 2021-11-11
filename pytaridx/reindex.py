## Written by Tomas Oppelstrup as part of the DOE/NCI Pilot-2 project.
## Updated to support python 3 by Francesco DiNatale.

# SPDX-License-Identifier: MIT

import sys
import pytaridx

# ------------------------------------------------------------------------------
def main():

    if len(sys.argv) <= 1:
        ustring = "Usage: python reindex.py file1.tar [file2.tar file3.tar ...]"
        ustring += "\n\n"
        ustring += "This program (re-)creates indices for the tar files named on the command line.\n"
        print(ustring)

    else:
        T = pytaridx.IndexedTarFile()
        print(f"Creating new indixes for {len(sys.argv)-1} tar files.")
        for x in sys.argv[1:]:
            print(f"Creating index for file ({x})...")
            T.reindex(x)
        print("Finished.")

if __name__ == '__main__':
    main()

# ------------------------------------------------------------------------------
