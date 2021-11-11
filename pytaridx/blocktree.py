## Written by Tomas Oppelstrup as part of the DOE/NCI Pilot-2 project.
## Updated to support python 3 and to become a module by Francesco DiNatale.

# SPDX-License-Identifier: MIT

import os

from .block import Block

## The BlockTree class implements a b-tree like data structure to
## maintain an index (each item is a (name,offset,size) triplet. The
## index is kept on disk using the Block data structure (from
## Block.py). Blocks are kept in duplicate (most recent and prior
## versions) do allow some defense against data curruption. This
## interface supports multiple readers concurrent with up to one
## writer.
class BlockTree:
    def __init__(self, filename, mode, cacheflag=False, allowoverwrite=True):
        self.backing = self.BlockFile(filename, mode, cacheflag)
        self.backing.overwriteflag = allowoverwrite
        items = self.backing.readblock(1)
        self.root = self.Node(self.backing, items)

    class BlockFile:
        masterblocksize = 512

        def __init__(self, filename, mode, cacheflag):
            self.filename = filename
            self.mode = mode
            self.cacheflag = cacheflag
            self.dirtymap = {}

            if mode == "r":
                self.readonly = True
            elif mode == "r+":
                self.readonly = False
            else:
                raise Exception(
                    "BlockTree: Unknown open mode '{}' for file '{}'"
                    .format(filename, mode)
                )

            self.filehandle = open(filename, mode + "b")
            self.readmasterblock()
            pos = self.filehandle.tell()
            self.filehandle.seek(0, os.SEEK_END)
            endpos = self.filehandle.tell()
            self.filehandle.seek(pos)
            self.freeblock = endpos // self.blocksize
            if self.freeblock*self.blocksize != endpos:
                msg = \
                    "Size of index file {} not divisible by blocksize " \
                    "({},{})" \
                    .format(filename, endpos, self.blocksize)
                raise Exception(msg)
            self.lastblock = (self.freeblock - 1)//2
            self.freeblock = self.lastblock + 1

        def allocateblock(self):
            blk = self.freeblock
            self.freeblock = blk + 1
            return blk

        @classmethod
        def writemasterblock(cls, fname):
            # TODO: why these numbers?
            blocksize = 1024  # 4096
            maxitems = 100
            maxnamelen = 160
            maxreclen = maxnamelen + 2 * 15 + 3

            if cls.masterblocksize > blocksize:
                msg = \
                    "Blocksize ({}) must be larger tham masterblocksize " \
                    "({})!"\
                    .format(blocksize, cls.masterblocksize)
                raise Exception(msg)

            items = {
                "blocksize":   blocksize,
                "maxitems":    maxitems,
                "maxnamelen":  maxnamelen,
                "maxreclen":   maxreclen
            }
            blk = Block(items, cls.masterblocksize, 0)
            fp = open(fname, "wb")
            fp.write(blk.data)
            fp.write(
                bytearray(
                    "\x00" * (blocksize - len(blk.data)),
                    encoding="ASCII"
                )
            )
            n = BlockTree.Node(None, None)
            n.blockno = 1
            items = n.blockitems()
            items["seqno"] = 1
            blk = Block(items, blocksize, 1)
            fp.write(blk.data)
            fp.write(blk.data)

            fp.close()

        def readmasterblock(self):
            self.filehandle.seek(0)
            info = Block(self.filehandle, self.masterblocksize, 0)
            if not info.valid:
                raise Exception(
                    "Master block of file {} invalid!".format(self.filename))

            items = info.parse()
            self.blocksize = items["blocksize"]
            self.maxitems = items["maxitems"]
            self.maxnamelen = items["maxnamelen"]
            self.maxreclen = items["maxreclen"]

        def getbothblocks_(self, blockno):
            b1 = Block(self.filehandle, self.blocksize, 2 * blockno - 1)
            b2 = Block(self.filehandle, self.blocksize, 2 * blockno)
            s1 = -1
            s2 = -1
            i1 = {}
            i2 = {}

            if b1.valid:
                i1 = b1.parse()
                s1 = i1["seqno"]
                if i1["blockno"] != blockno or s1 <= 0:
                    raise Exception(
                        "Incorrect block number in block ({}, {}, {})!"
                        .format(i1["blockno"], blockno, s1)
                    )
            if b2.valid:
                i2 = b2.parse()
                s2 = i2["seqno"]
                if i2["blockno"] != blockno or s2 <= 0:
                    raise Exception(
                        "Incorrect block number in block ({}, {}, {})!"
                        .format(i2["blockno"], blockno))
            return (i1, i2, s1, s2)

        def readblock(self, blockno):
            (i1, i2, s1, s2) = self.getbothblocks_(blockno)
            if s1 > s2:
                return i1
            elif s2 > 0:
                return i2
            else:
                raise Exception(
                    "Neither primary nor backup block valid! ({}:{}:{})"
                    .format(self.filehandle, self.blocksize, blockno)
                )

        def writeblock(self, blockno, items):
            writeboth = blockno > self.lastblock
            if blockno <= self.lastblock:
                (i1, i2, s1, s2) = self.getbothblocks_(blockno)
                if s1 > s2:
                    s0 = s1
                    offset = 2*blockno
                elif s2 > 0:
                    s0 = s2
                    offset = 2*blockno-1
                else:
                    raise Exception(
                        "Neither primary nor backup block valid! ({}:{}:{})"
                        .format(self.filehandle, self.blocksize, self.blockno)
                    )
            elif blockno > self.lastblock + 1:
                msg = \
                    "Writing more than one block beying end of file ({},{})" \
                    .format(blockno, self.lastblock)
                raise Exception(msg)
            else:
                s0 = 0
                offset = 2*blockno-1

            c = dict(items)
            c["seqno"] = s0 + 1

            b0 = Block(c, self.blocksize, blockno)
            self.filehandle.seek(self.blocksize * offset)
            self.filehandle.write(b0.data)
            if writeboth:
                self.filehandle.write(b0.data)
                self.lastblock = blockno
            self.filehandle.flush()

        def setdirty(self, node):
            if self.cacheflag:
                self.dirtymap[node] = True
            else:
                blkitems = node.blockitems()
                self.writeblock(node.blockno, blkitems)

        def flushcache(self):
            if self.cacheflag:
                for n in self.dirtymap:
                    blkitems = n.blockitems()
                    self.writeblock(n.blockno, blkitems)
                self.dirtymap = {}

    def flushtree(self):
        self.root.flushsubtree()
        self.root = None
        items = self.backing.readblock(1)
        self.root = self.Node(self.backing, items)

    def insert(self, key, value):
        if len(key) > self.backing.maxnamelen:
            raise Exception(
                "Insert: key too long ({},{})!"
                .format(len(key), self.backing.maxnamelen)
            )
        self.root = self.root.insert(key, value)

    def last(self):
        return self.root.last()

    def lookup(self, key):
        return self.root.lookup(key)

    def check(self, loadfromdisk=False):
        self.root.treecheck(1, loadfromdisk)

    class Node:
        def __init__(self, backing, items):
            self.backing = backing
            self.parent = None
            if items is None:
                self.blockno = -1
                self.leaf = True
                self.items = []
                self.nodeptrs = []
            else:
                self.blockno = items["blockno"]
                self.leaf = (items["leaf"] > 0)
                self.items = list(items["items"])
                if self.leaf:
                    self.nodeptrs = []
                else:
                    self.nodeptrs = [None for x in self.items]

        def flushsubtree(self):
            if not self.leaf:
                for i in range(len(self.nodeptrs)):
                    if self.nodeptrs[i] is not None:
                        self.nodeptrs[i].flushsubtree()
                        self.nodeptrs[i] = None

        def blockitems(self):
            is_leaf = 1 if self.leaf else 0
            blkitems = {
                "leaf":     is_leaf,
                "blockno":  self.blockno,
                "items":    list(self.items)
            }
            return blkitems

        def storesize(self):
            blkitems = self.blockitems()
            return Block.dataoffset + len(Block.assignstring_(blkitems))

        def findroot(self):
            p = self
            while p.parent is not None:
                p = p.parent
            return p

        def adjust(self):
            freespace = self.backing.blocksize - self.storesize()
            if len(self.items) >= self.backing.maxitems or \
               freespace < self.backing.maxreclen:
                self.split()
                if self.parent is not None:
                    self.parent.adjust()

        def split(self):
            right = BlockTree.Node(self.backing, None)
            right.leaf = self.leaf
            n = len(self.items)
            p = n//2
            right.items = self.items[p:n]
            if not right.leaf:
                right.nodeptrs = self.nodeptrs[p:n]
                for x in right.nodeptrs:
                    if x:
                        x.parent = right

            ##++ We are now allocating a new block both for left and
            ##   right children after splitting, so that we can not have a
            ##   race condition on the children being updated before the
            ##   parent. If the old child node is retained (stale), it
            ##   will still allow a consistent walk of the tree to find
            ##   all members except the one added which caused the split.
            oldselfblock = self.blockno  #++ Keep track of old block number
            self.blockno = self.backing.allocateblock()  #++ Before, new block was not allocated
            right.blockno = right.backing.allocateblock()

            del self.items[p:n]
            if not self.leaf:
                del self.nodeptrs[p:n]

            newroot = bool(self.parent is None)
            if(self.parent is None):
                #++ Not allocating blocks here, since it is done earlier already
                # Reassigning the newly allocated right block creates
                # a more orders tree structure...
                #-- self.blockno = right.blockno
                #-- right.blockno = self.backing.allocateblock()

                # print "newroot: blknos: self = %d, right = %d, root = %d" \
                #    % (self.blockno, right.blockno,1)

                root = BlockTree.Node(self.backing, None)
                root.leaf = False
                root.items.insert(0, (self.items[0][0], self.blockno))
                root.items.insert(1, (right.items[0][0], right.blockno))
                root.nodeptrs.insert(0, self)
                root.nodeptrs.insert(1, right)

                #++ Added consistency check
                if oldselfblock != 1:
                    raise Exception("Inconsistent tree -- root block should be no. 1, but is %d !" \
                                    % (oldselfblock,))

                root.blockno = 1
                self.parent = root
                right.parent = root
            else:
                idx = self.parent.find(right.items[0][0])
                self.parent.items.insert(
                    idx,
                    (right.items[0][0], right.blockno)
                )
                self.parent.nodeptrs.insert(idx, right)
                right.parent = self.parent

            if newroot:
                self.backing.setdirty(self)
                self.backing.setdirty(right)
                self.backing.setdirty(self.parent)
            else:
                #-- self.backing.setdirty(right)
                #-- self.backing.setdirty(self.parent)
                #-- self.backing.setdirty(self)
                ##++ Swapping writing parent to last, since both
                ##   left and right children are new blocks now.
                self.backing.setdirty(self)         #++
                self.backing.setdirty(right)        #++
                self.backing.setdirty(self.parent)  #++

        def find(self, key):
            n = len(self.items)
            if n < 8:
                # For short lists, linear search is the fastest
                for i in range(0, n):
                    if key < self.items[i][0]:
                        return i
                return n
            else:
                # For longer lists, use binary search
                a = -1
                b = n
                while b-a > 1:
                    k = (a+b)//2
                    if key < self.items[k][0]:
                        b = k
                    else:
                        a = k
                return b

        def insert(self, key, value):
            idx = self.find(key)
            if self.leaf:
                dirtylist = [self]
                owrt = False
                if idx > 0:
                    if self.items[idx-1][0] == key:
                        if self.backing.overwriteflag:
                            owrt = True
                        else:
                            raise Exception(
                                "Object ({}, {}) already in tree!"
                                .format(str(key), str(value)))
                else:
                    p = self.parent
                    while p is not None:
                        # Walk up tree to update parent values if
                        # the first element of this node was inserted
                        oldkey = self.items[0][0]
                        idx2 = p.find(oldkey)
                        p.items[idx2-1] = (key, p.items[idx2-1][1])
                        dirtylist.append(p)
                        if idx2 == 1:
                            p = p.parent
                        else:
                            break

                info = (key,) + value if type(value) == tuple else (key, value)
                if owrt:
                    self.items[idx-1] = info
                else:
                    self.items.insert(idx, info)

                for n in dirtylist:
                    self.backing.setdirty(n)
            else:
                if idx == 0:
                    idx = 1
                if not self.nodeptrs[idx-1]:
                    b = self.backing.readblock(self.items[idx-1][1])
                    n = BlockTree.Node(self.backing, b)
                    n.parent = self
                    self.nodeptrs[idx-1] = n

                self.nodeptrs[idx-1].insert(key, value)

            self.adjust()
            return self.findroot()

        def lookup(self, key):
            idx = self.find(key)
            if idx <= 0:
                raise Exception("%s not found!" % str(key))
            if self.leaf:
                if self.items[idx-1][0] == key:
                    # print "Lookup: ", key, self.items[idx-1][1:]
                    return self.items[idx-1][1:]
                else:
                    raise Exception("%s not found (leaf)!" % str(key))
            else:
                if not self.nodeptrs[idx-1]:
                    b = self.backing.readblock(self.items[idx-1][1])
                    n = BlockTree.Node(self.backing, b)
                    n.parent = self
                    self.nodeptrs[idx-1] = n
                return self.nodeptrs[idx-1].lookup(key)

        def last(self):
            p = self
            while p is not None and not p.leaf:
                i = len(p.items) - 1
                if not p.nodeptrs[i]:
                    b = p.backing.readblock(p.items[i][1])
                    n = BlockTree.Node(p.backing, b)
                    n.parent = p
                    p.nodeptrs[i] = n
                p = p.nodeptrs[i]

            if p is None:
                raise Exception("No leaf at bottom of tree!")

            return p.items[-1]

        def treecheck(self, level, loadfromdisk):
            for i in range(1, len(self.items)):
                if self.items[i-1] >= self.items[i]:
                    raise Exception("Unordered!")

            if self.parent is not None:
                # if len(self.items) < self.backing.maxitems/2:
                #    raise Exception("Item list too short")
                idx = self.parent.find(self.items[0][0])
                if self.parent.items[idx-1][0] != self.items[0][0]:
                    # print("Level = ", level)
                    # print("items: ", self.items)
                    # print("parent items: ", self.parent.items)
                    # print("First item: ", self.items[0][0])
                    # print("Index in parent: ", idx)
                    raise Exception("Incorrect parent list (1)")

                if idx < len(self.parent.items):
                    if self.items[-1][0] >= self.parent.items[idx][0]:
                        raise Exception("Incorrect parent list (2)")
                if self.parent.nodeptrs[idx-1] != self:
                    raise Exception("Incorrect pointer in parent")

            if not self.leaf:
                if loadfromdisk:
                    for i in range(len(self.items)):
                        if not self.nodeptrs[i]:
                            block = self.backing.readblock(self.items[i][1])
                            n = BlockTree.Node(self.backing, block)
                            n.parent = self
                            self.nodeptrs[i] = n

                for x in self.nodeptrs:
                    if x:
                        x.treecheck(level + 1, loadfromdisk)


def printtree(root, s):
    if root.leaf:
        for x in root.items:
            print("This is x :: {}".format(str(x)))
            print("{:.6f} -> {}".format(s, x[0], x[1]))
    else:
        for x in root.items:
            print("{:.6f}:".format(s, x[0]))
            printtree(x[1], s + "    ")


def printtreex(root, s):
    print(
        "%s Node with leaf = %d, %d element"
        % (s, root.leaf, len(root.items))
    )
    if not root.leaf:
        for x in root.items:
            printtreex(x[1], s + "    ")


# Main
if __name__ == "__main__":
    import random

    random.seed(263541)

    BlockTree.BlockFile.writemasterblock("bfile.1")

    # print("Creating index tree file...")
    tree = BlockTree("bfile.1", "r+")

    # print("Inserting (random) data...")
    vec = []
    for i in range(2000):
        key = "%.12f" % random.random()
        value = (i, i * i)
        vec.append(key)
        # print "Tree after insert of %.6f:" % (key)
        tree.insert(key, value)
        # print "Current tree:"
        # printtree(root,"  ")
        # print i
        # printtree(root,"  ")
        # treecheck(root,1)

    # printtree(root,"")
    # print("Self consistency check...")
    tree.check()

    # print("Closing file, and reopening...")
    tree.backing.filehandle.close()
    tree = BlockTree("bfile.1", "r")

    # print("Check that all inserted values are present...")
    for i in range(len(vec)):
        k = tree.lookup(vec[i])
        # if k != (i, i * i):
        #    print(
        #       "%d (k = %d,%d) not found at %.8f" % (i, k[0], k[1], vec[i]))
