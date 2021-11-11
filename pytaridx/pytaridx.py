# Version one of python iterface for reading and appending tar
# files, while keeping a fast index for finding and reading
# files in the archive.
#
# Written by Tomas Oppelstrup as part of the DOE/NCI Pilot-2 project,
# based in nitial (simpler) index implementation by Tomas Oppelstrup,
# December 11, 2018. Updated to support python 3 and to become a
# module by Francesco DiNatale.
#
# SPDX-License-Identifier: MIT
#
# Interface functions:
#
#     itf = IndexedTarFile()
#       Creates a handle (object) to represent an indexed tar file.
#
#     itf.open(filename,mode = {"r","r+"}): Opens filename as a tar
#        file in read-only ("r") or read-and-append ("r+") mode.
#        write, and random access read mode. If no tar file is found,
#        and it is not opened on read-only mode, an empty archive is
#        created. An index file with name filename + ".pytree" is
#        loaded for quick read access to archive members (files).  If
#        no index file is found, and the file is opened for writing
#        (read-and-append), a new index is created. Besides the index
#        file, and additonal list file (filename + ".pylst") kept,
#        which contains a list of all members together with their
#        offset in the tar file and their size. If the file is opened
#        for reading only and the tar file or its index files are
#        missing, an exception is raised.
#
#     flag = itf.exist(objname):
#        Checks if there is a member (file) with name objname
#        in the archive (using the index file).
#
#     objname = itf.last():
#        Returns the name of the last member (file) added to the
#        archive.  This is located using the list file.
#
#     data = itf.read(objname):
#        Finds member (file) with name objname in index and reads it
#        from tar file. If there are multiple members in the tar file
#        with this same name, the last one will be read.
#
#     itf.write(objname,data)
#        Appends a member (file) with name objname and contents data
#        to tar file, also updates index to find this data on a future
#        read of objname. I.e. read finds the last member (file) in
#        the archive of a given name.
#
#     data = itf.readlist(objnamelist):
#        Reads each object named in objnamelist and return a list of
#        data objects.
#
#     itf.writelist(objnamelist,datalist):
#        Does a write on each name,data pair in objnamelist,datalist.
#
#     its.close():
#        Closes the archive and associated index files.
#
# If this program is run as a main program (e.g. python pytaridx.py),
# then a test is performed which writes several (10 items) to an
# archive, and then performs several (100) retrievals of randomly
# selected items. The test typically takes a second or less on a
# workstation.

"""A module containing a class for producing and reading tar archives
with efficient indexes to allow radom (read) access."""

import tarfile
import random
import time
import os
import re

from .blocktree import BlockTree


class IndexNotFoundError(Exception):
    pass


class IndexedTarFile:
    """Main class that implements an API to creating, random access reading,
    writing, and re-indexing of indexed tar files."""

    class _IndexManager:
        """
        Index interface data structure that manages the index files, using
        other classes as backends. In this implementation two files are
        kept: a b-tree for quick lookup of any member by name, and a
        linear list that is only appended to. The linear list can be used
        to quickly probe for the last member(s) added, and for fast
        re-indexing.
        """
        def __init__(self, tarfilebasename, mode):
            self.basename = tarfilebasename
            self.treename = self.basename + ".pytree"
            self.lstname = self.basename + ".pylst"

            if not (mode == "r" or mode == "r+"):
                msg = \
                    "Invalid mode flag to IndexedTarFile._IndexManager (%s)" \
                    % mode
                raise Exception(msg)

            self.readonly = (mode == "r")

            self.tree = BlockTree(self.treename, mode)
            if self.readonly:
                self.lstfile = open(self.lstname, mode + "b")
            else:
                self.lstfile = open(self.lstname, "a+b")

            self.maxreclen = self.tree.backing.maxreclen

            if not self.lstfile:
                msg = "Unable to open list file (%s,%s)" % (self.lstfile, mode)
                raise Exception(msg)

        def last(self):
            n = 2*self.maxreclen + 1
            f = self.lstfile
            try:
                f.seek(-n, os.SEEK_END)
            except Exception:
                f.seek(0, os.SEEK_END)
                sz = f.tell()
                if n > sz:
                    n = sz
                f.seek(sz-n, os.SEEK_SET)

            data = f.read(n)
            if f.tell() == 0:
                return None

            lines = data.splitlines()
            # print "Last lines = \n",lines
            # Parse last line, to obtain member name and location info
            pat = b"(\\\\.|[^,])*(,|\n)"
            patc = re.compile(pat)
            spat = b'\\\\(.)'
            spatc = re.compile(spat)

            foundit = False
            for i in [-1, -2]:
                rec = lines[i]
                # print("rec = %s"%(rec,))
                try:
                    elems = [spatc.sub(b'\1', x.group()[:-1])
                             for x in patc.finditer(rec + b"\n")]
                    # print("Elems = ")
                    # print(elems)
                    elems[0] = elems[0].decode("ASCII")
                    # print("elem0 = %s" %(elems[0],))
                    for i in range(1, len(elems)):
                        elems[i] = int(elems[i])
                    if len(elems) == 3:
                        foundit = True
                        break
                except Exception:
                    # When the index package is asked to find the last member
                    # it reads the last chuck of this list file. Nearly
                    # always, the last line will contain the relevant 
                    # information. However, in some uncommon cases another 
                    # process will be writing (appending) to this file at the
                    # same time. In this case the last line could be garbage 
                    # (unlikely but possible). It is in this case that the
                    # parsing may fail, for instance by on of its statements
                    # throwing an exception. In this case, I want to move on to
                    # the second to last line. I don't really care what
                    # exception the garbage line caused. If the garbage line
                    # causes an exception that was not anticipated, then it 
                    # will take down the whole program, which is not what we
                    # want, since reading the second to last line is an
                    # acceptable solution. Therefore I really care about
                    # catching all exceptions in this piece of code.
                    pass

            if not foundit:
                raise Exception("Incorrect data at end of lstfile '%s':%s\n"
                                % (self.lstname, data.decode("ASCII")))

            return tuple(elems)

        def lookup(self, name):
            return self.tree.lookup(name)

        def exist(self, name):
            try:
                self.lookup(name)
                return True
            except Exception:
                pass

            return False

        def insert(self, name, offset, size):
            if self.readonly:
                msg = \
                    "Attempting to insert (%s,%d,%d) to read-only index %s" % \
                    (name, offset, size, self.treename)
                raise Exception(msg)

            rec = re.compile("(,|\\\\)")
            info = bytearray(
                "%s,%d,%d\n" % (rec.sub(r'\\\1', name), offset, size),
                encoding="ASCII")
            f = self.lstfile
            f.seek(0, os.SEEK_END)
            f.write(info)
            f.flush()

            self.tree.insert(name, (offset, size))

    class _IndexStruct:
        """
        Data structure held by tar file index, holds e.g. object size and
        offset in tar file.
        """

        def __init__(self, offset_or_tuple, size=None):
            """
            Initialize a new instance of the _IndexStruct class.

            :param offset: <description>
            :param size: <description>
            """
            if size is None:
                self.offset = offset_or_tuple[0]
                self.size = offset_or_tuple[1]
            else:
                self.offset = offset_or_tuple
                self.size = size

        def totuple(self):
            return (self.offset, self.size)

    def __init__(self):
        """Initialize a new instance of an IndexedTarFile"""
        self.isopen = 0
        self.filename = ""

    def open(self, name, mode="r"):
        """
        Open an indexed tar file.

        :param name: String name of the tar archive.
        :param mode: File mode: "r" = read-only, "r+" = read and append
        """

        if mode == "r":
            self.readonly = True
        elif mode == "r+":
            self.readonly = False
        else:
            raise Exception(
                "Unknown mode %s for tar archive %s" % (mode, name))

        self.filename = name
        haveindex = False
        try:
            self.index = self._IndexManager(self.filename, mode)
            haveindex = True
        except Exception:
            if self.readonly:
                message = \
                    "Index file for tar file located at '{}' not found." \
                    .format(os.path.abspath(name))
                raise IndexNotFoundError(message)

        if self.readonly:
            self.tfr = open(name, "rb")
            self.tfr.seek(0, os.SEEK_END)
            self.tfa = tarfile.open(name, "r", self.tfr, 512)
        else:
            self.tfr = open(name, "a+b")
            # "w|" is used instead of "a", to make this work on
            # macs. Sadly, "w|" has internal buffering, and we can't know
            # that it gets flushed. For that reason, in write(), we write
            # explicitly to the underlying file, while using the tarfile
            # interface to construct the approporiate header...
            self.tfr.seek(0, os.SEEK_END)
            self.tfa = tarfile.open(name, "w|", self.tfr, 512)

        if not haveindex:
            # print("Unable to read index, regenerating!")
            if not self.readonly:
                self.reindex()
                self.index = self._IndexManager(self.filename, mode)

        self.isopen = 1

    def close(self):
        """Close the tar archive and reindex the index on close."""
        self.tfa.closed = True
        self.tfa.close()
        self.tfr.close()

        self.index = None
        self.backing = None
        self.isopen = 0

    def reindex(self, tarfilename=None):
        """
        Re-create the tar index.

        :param tarfilename (optionally): Re-build the index for the
        indexed tar-file in self, or for the tar file named by
        tarfilename if that parameter is given.
        """
        if tarfilename is None:
            self.tfr.seek(0)
            tarobj = self.filename
            fp = self.tfr
        else:
            tarobj = tarfilename
            fp = None

        treename = tarobj + ".pytree_"
        lstname = tarobj + ".pylst_"
        BlockTree.BlockFile.writemasterblock(treename)
        lstfp = open(lstname, "w")
        index = BlockTree(treename, "r+", False)
        count = 0

        # rec is for escaping , (comma)  and backslash (\)
        rec = re.compile("(,|\\\\)")
        try:
            with tarfile.open(tarobj, 'r|', fp) as tf:
                for info in tf:
                    count = count + 1
                    index.insert(info.name, (info.offset_data, info.size, ))
                    escname = rec.sub(r'\\\1', info.name)
                    lstfp.write("%s,%d,%d\n" %
                                (escname, info.offset_data, info.size))
                    if (count % 10000) == 0:
                        index.flushtree()
        except tarfile.ReadError:
            pass

        index.backing.flushcache()
        index.flushtree()
        lstfp.close()

        os.rename(lstname, lstname[:-1])
        os.rename(treename, treename[:-1])

    def exist(self, objname):
        """
        Check if there is a file objname in the archive.

        :param objname: Name of object (file, member) to look up
        """
        return self.index.exist(objname)

    def last(self):
        """
        Return the name o the last member (file) added to the archive.
        """
        return self.index.last()

    def read(self, objname):
        """
        Read a file from the archive.

        :param objname: Name of object (file in tararchive). This will be
         looked up in index and read from tar file. Will throw
         exception if member not found.
        """
        objdata = self.index.lookup(objname)
        # print "Read: ",objname,objdata
        info = self._IndexStruct(objdata)
        oldpos = self.tfr.tell()
        self.tfr.seek(info.offset)
        buf = self.tfr.read(info.size)
        self.tfr.seek(oldpos)
        return buf

    def write(self, objname, data):
        """
        Write a file to the archive.

        :param objname: Name of new object, which will be a file with this
                        name in the tar archive.
        :param data: A buffer of data that acts like a bytes or bytearray.
                     The size should be len(data).
        """
        oldpos = self.tfr.tell()
        self.tfr.seek(0, os.SEEK_END)
        offset = self.tfr.tell() + 512

        # Set name, object size, and creation/modification time of new object:
        info = self.tfa.gettarinfo(self.index.treename)
        info.name = objname
        info.size = len(data)
        info.mtime = time.time()

        # Add new object to tar file:
        # New code, explicit write to tarfile to make sure flush()
        # works in "w|" mode (see open()), and nothing remains in
        # internal python/libc buffering after write returns.
        infobuf = \
            info.tobuf(self.tfa.format, self.tfa.encoding, self.tfa.errors)
        self.tfr.write(infobuf)
        self.tfr.write(data)
        q, r = divmod(len(data), 512)
        if r > 0:
            pad = 512 - r
            self.tfr.write(bytearray("\0" * pad, encoding="ASCII"))
        # Make sure data is committed to operationg system
        self.tfr.flush()
        self.tfr.seek(oldpos)

        self.index.insert(objname, offset, len(data))

    def readlist(self, namelist):
        """
        Read a list of files from the archive.

        :param namelist: <description>
        :returns: <description>
        """
        objlist = []
        for i in range(0, len(namelist)):
            objlist.append(self.read(namelist[i]))
        return objlist

    def writelist(self, objnamelist, datalist):
        """
        Write a list of files to the archive.

        :param objnamelist: <description>
        :param datalist: <description>
        """
        for i in range(0, len(objnamelist)):
            self.write(objnamelist[i], datalist[i])


if __name__ == "__main__":
    ## When used as main program, performs some writing and reading
    ## tests, and checks correctness of the index.
    def idxcheck(map1, map2):
        print("Index check.")
        print("Checking for 1 in 2")
        for k in map1:
            if k in map2:
                if map1[k] != map2[k]:
                    print(
                        "k = {} map1 = {}"" map2 = {}"
                        .format(k, map1[k], map2[k])
                    )
            else:
                print("'{}' missing in map2".format(k))

        print("Checking for 2 in 1")
        for k in map2:
            if k in map2:
                if map1[k] != map2[k]:
                    print(
                        "k = {} map1 = {}"" map2 = {}"
                        .format(k, map1[k], map2[k])
                    )
            else:
                print("'{}' missing in map1".format(k))

    # Test code to exercise reading and writing
    print("Opening tarfile newfile.tar...")
    itf = IndexedTarFile()
    itf.open("newfile.tar", "r+")

    print("Checking for last entry")
    name = itf.last()
    print("  Last added object is: {}".format(name))

    print("Writing objects...")
    nobj = 10
    niter = 100
    t0 = time.time()
    for i in range(0, nobj):
        buf = "File id = {}, data = {}.".format(
                i, random.randint(1000000, 10000000)
              )

        # Uncomment to make larger objects, in this case exaclty 512 bytes:
        # q,r = divmod(len(buf),512)
        # if r > 0:
        #     buf = buf + ("#" * (512-r))

        name = "obj-no-{}".format(i)
        # print "Writing '%s' := '%s'" % (buf,name)
        itf.write(name, bytearray(buf, encoding='ASCII'))

    t1 = time.time()
    print(
        "Writing took {:.3f} s, or {:.3f} us / write."
        .format(t1 - t0, (t1 - t0) / nobj * 1e6)
    )

    print("Reading objects...")
    t0 = time.time()
    for iter in range(0, niter):
        i = random.randint(0, nobj-1)

        name = "obj-no-{}".format(i)
        # print "Reading '%s'..." % (name)
        buf = itf.read(name)
        # print "  --> '%s'" % (buf)

    t1 = time.time()
    print(
        "Reading took {:.3f} s, or {:.3f} us / access."
        .format(t1 - t0, (t1 - t0) / niter * 1e6)
    )

    print("Checking object existence...")
    count = 0
    failcount = 0
    t0 = time.time()
    for iter in range(0, niter):
        i = random.randint(0, 2*nobj-1)

        name = "obj-no-%d" % i
        # print "Reading '%s'..." % (name)
        if itf.exist(name):
            if i < nobj:
                count += 1
            else:
                failcount += 1
        else:
            if i < nobj:
                failcount += 1
            else:
                count += 1

    t1 = time.time()
    print(
        "Existence checking took {:.3f} s, or {:.3f} us / access "
        "(count = {}, failcount = {})."
        .format(t1 - t0, (t1 - t0) / niter * 1e6, count, failcount)
    )

    print("Checking for last entry")
    name = itf.last()
    print("  Last added object is: {}".format(name))

    print("Closing file...")
    itf.close()
    print("Finished.")
