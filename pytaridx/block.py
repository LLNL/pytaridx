## Written by Tomas Oppelstrup as part of the DOE/NCI Pilot-2 project.
## Updated to support python 3 and to become a module by Francesco DiNatale.

# SPDX-License-Identifier: MIT

import hashlib
import json
import re
import time

# The Block class implements a block that can hold key-value pairs and
# a list of items (name,offset,size). Each block is protected by a
# sha256 hashsum to ensure data integrity. When used as a main
# program, this file conducts some correctness and performance tests.

class Block:
    hashsize = -1
    hashoffset = -1
    dataoffset = -1

    header = ""

    def __init__(self, filehandle, blksize, blkno):
        self.blocksize = blksize
        self.blkno = blkno

        if Block.hashsize < 0:
            # print("Initializing Block global data")
            m = hashlib.sha256()
            Block.hashsize = 2 * m.digest_size
            Block.header = "hash = " + " " * Block.hashsize + "\n"
            Block.hashoffset = 7
            Block.dataoffset = len(Block.header)

        if filehandle is None:
            self.data = bytearray(
                Block.header + "\x00" * (self.blocksize - Block.dataoffset),
                encoding="ASCII"
            )
            info = "end\n"
            p = Block.dataoffset
            self.data[p:(p + len(info))] = info
            self.makehash()
        elif type(filehandle) == dict:
            self.data = bytearray(
                Block.header + "\x00" * (self.blocksize - Block.dataoffset),
                encoding="ASCII"
            )
            self.assign(filehandle)
        else:
            offset = self.blkno * self.blocksize
            filehandle.seek(offset)
            self.data = bytearray(filehandle.read(self.blocksize))

        self.valid = self.checkhash()

    def checkhash(self):
        m = hashlib.sha256()
        start = Block.hashoffset
        end = Block.hashoffset + Block.hashsize

        hash1 = self.data[start:end]
        self.data[start:end] \
            = bytearray(" " * Block.hashsize, encoding="ASCII")
        m.update(self.data)
        hash2 = bytearray(m.hexdigest(), encoding="ASCII")
        self.data[start:end] = hash1

        # print("Hash check:")
        # print("  ", hash1)
        # print("  ", hash2)
        # print("h1 == h2: ", hash1 == hash2)
        # return cmp(hash1, hash2) == 0
        return bool(hash1 == hash2)

    def makehash(self):
        m = hashlib.sha256()
        start = Block.hashoffset
        end = Block.hashoffset + Block.hashsize

        self.data[start:end] = \
            bytearray(" " * Block.hashsize, encoding="ASCII")
        m.update(self.data)
        hash = m.hexdigest()
        self.data[start:end] = bytearray(hash, encoding="ASCII")

    def printblock(self):
        for line in self.data.splitlines():
            print(line)
            if line == "end":
                break

    def parse(self):
        items = {}
        listmode = False

        pat = b"(\\\\.|[^,])*(,|\n)"
        patc = re.compile(pat)
        spat = b'\\\\(.)'
        spatc = re.compile(spat)

        for line in self.data.splitlines():
            if listmode:
                # print("line = ", line + b"\n")
                if line == b"}":
                    items[curname] = curlist
                    listmode = False
                else:
                    elems = [
                        spatc.sub(b'\1', x.group()[:-1])
                        for x in patc.finditer(line + b"\n")
                    ]
                    elems[0] = elems[0].decode("ASCII")
                    for i in range(1, len(elems)):
                        elems[i] = int(elems[i])
                    curlist.append(tuple(elems))
            else:
                tt = line.split(b' = ', 1)
                curname = tt[0].decode("ASCII")
                if curname == "end":
                    break
                else:
                    tt[1] = tt[1].decode("ASCII")
                    if tt[1] == "{":
                        curlist = []
                        listmode = True
                    elif curname != "hash":
                        items[curname] = int(tt[1])

        return items

    @classmethod
    def assignstring_(cls, items):
        rec = re.compile("(,|\\\\)")

        def serialize(rhs):
            if type(rhs) == list:
                if len(rhs) > 0:
                    return "{\n" + \
                        "\n".join(
                            [",".join(
                                [rec.sub(r'\\\1', str(col))
                                    for col in row])
                                for row in rhs]) \
                        + "\n}"
                else:
                    return "{\n}"
            else:
                return "%d" % rhs

        return "\n".join(
            ["%s = %s" % (k, serialize(items[k])) for k in items]) \
            + "\nend\n"

    def assign(self, items):
        pos = Block.dataoffset

        itemstr = Block.assignstring_(items)
        n = len(itemstr)
        if Block.dataoffset + n > self.blocksize:
            # raise Exception("Item array to large to fit in block")
            return False
        else:
            self.data[pos:(pos + n)] = bytearray(itemstr, encoding="ASCII")
            self.makehash()
            return True


if __name__ == "__main__":

    items = {
        "nitems":   0,
        "leaf":     1,
        "parent": -99,
        "items": [
            ("one", 1),
            ("two", 2),
            ("three", 3),
            ("j,,u\\nk\\,", 31, 19)
        ]
    }

    blk = Block(None, 4096, 0)
    # print(items)
    blk.assign(items)
    # blk.printblock()
    items2 = blk.parse()

    s1 = set([k for k in items])
    s2 = set([k for k in items2])
    if s1 != s2:
        # print("s1 = ", s1)
        # print("s2 = ", s2)
        raise Exception("s1 != s2")

    # print("Checking equality of items and items2...")
    # for k in s1:
    #   if items[k] != items2[k]:
    #       print("key    = ", k)
    #       print("  items  = ", items[k])
    #       print("  items2 = ", items2[k])

    niter = 10000
    tx = time.time()
    for k in range(niter):
        blk = Block(None, 4096, 0)

    tx = time.time() - tx

    # blk.printblock()
    # print("Construct time = %.3f us" % (tx/niter*1e6))

    t0 = time.time()
    for i in range(niter):
        blk.makehash()

    t1 = time.time()
    for i in range(niter):
        blk.checkhash()
    t2 = time.time()

    t3 = time.time()
    for i in range(niter):
        blk.parse()
    t4 = time.time()

    t5 = time.time()
    for i in range(niter):
        blk.assign(items)
    t6 = time.time()

    t7 = time.time()
    for i in range(niter):
        s = Block.assignstring_(items)
    t8 = time.time()
    ss = s

    t9 = time.time()
    for i in range(niter):
        s = json.dumps(items, separators=(',', ':'))
    t10 = time.time()

    t11 = time.time()
    for i in range(niter):
        q = json.loads(s)
    t12 = time.time()

    # print(
    #   "t make = %.3f us, t check = %.3f us,
    #   t parse = %.3f us, t assign = %.3f us"
    #  % ((t1-t0)/niter*1e6,(t2-t1)/niter*1e6,
    # (t4-t3)/niter*1e6,(t6-t5)/niter*1e6))

    # print("json  data (%d) = " % len(s), s)
    # print("block data (%d) = " % len(ss), ss)

    # print("t assignstring = %.3f us" % ((t8-t7)/niter*1e6))
    # print("t write json   = %.3f us" % ((t10-t9)/niter*1e6))
    # print("t read  json   = %.3f us" % ((t12-t11)/niter*1e6))
