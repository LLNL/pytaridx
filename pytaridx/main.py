## Written by Francesco DiNatale as part of the DOE/NCI Pilot-2 project.

# SPDX-License-Identifier: MIT

## This is a script/program to (re-)build indexes for tar files, then
## usable with pytaridx. This program supports parallelism using the
## multiprocessing module in python, and can build several in
## parallel.
from argparse import ArgumentParser, RawTextHelpFormatter
import glob
from multiprocessing import Pool, Manager
import os
import sys

import pytaridx


def setup_parser():
    parser = ArgumentParser(
        prog="pytridx",
        description="A CLI for managing pytaridx generated tar files.",
        formatter_class=RawTextHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest='subparser')

    # Reindexing related command line utilities and arguments
    reindex = subparsers.add_parser(
        "reindex",
        help="Tools for reindexing pytaridx generated files."
    )
    reindex.add_argument("-n", "--nprocesses", type=int, default=1,
        help="Number of processes to use for reindexing [DEFAULT: %(default)d]"
    )
    reindex.add_argument("tarfiles", type=str, nargs="+",
        help="Paths or globs of pytaridx IndexedTarFiles to reindex."
    )
    reindex.set_defaults(func=reindex_tarfiles)

    return parser


def reindex_tarfiles(args):
    pool = Pool(processes=args.nprocesses)
    manager = Manager()
    queue = manager.Queue()

    for item in args.tarfiles:
        _f_path = os.path.abspath(item)
        if os.path.isfile(_f_path):
            queue.put(item)
            continue

        for path in glob.glob(_f_path):
            queue.put(path)

    pool.map(process_reindex, [queue for i in range(args.nprocesses)])

    print("Finished.")


def process_reindex(queue):

    while not queue.empty():
        _tar = os.path.abspath(queue.get())
        print("Processing '%s'..." % (_tar))

        try:
            _tree = pytaridx.IndexedTarFile()
            _tree.reindex(_tar)
        except Exception as exep:
            print("Failed to process '%s'." % (_tar))
            print("Exception: %s" % (exep.msg))
            continue


def main():
    parser = setup_parser()
    args = parser.parse_args()


    rc = args.func(args)
    sys.exit(rc)
    
