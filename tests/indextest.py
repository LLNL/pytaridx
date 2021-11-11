## Written by Tomas Oppelstrup as part of the DOE/NCI Pilot-2 project.
## Updated to support python 3 by Francesco DiNatale.

# SPDX-License-Identifier: MIT

## Testprogram to perform some correctness tests on the pytaridx module.
import os
import time
import random
from pytaridx import IndexedTarFile

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
print("TESTING TAR INDEX PACKAGE (PYTARIDX)")
print("* Opening tarfile newfile.tar...")
itf = IndexedTarFile()
itf.open("newfile.tar","r+")

print("* Checking for last entry")
name = itf.last()
print("  Last added object is: {}".format(name))

print("* Writing objects...")
nobj = 100
niter = 1000
t0 = time.time()
objmap = {}
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
    buf = bytearray(buf,encoding="ASCII")
    itf.write(name, buf)
    objmap[name] = buf
    lastname = name

t1 = time.time()
print(
    "  Writing took {:.3f} s, or {:.3f} us / write."
    .format(t1 - t0, (t1 - t0) / nobj * 1e6)
)

print("* Reading objects...")
t0 = time.time()
for iter in range(0, niter):
    i = random.randint(0, nobj-1)

    name = "obj-no-{}".format(i)
    # print "Reading '%s'..." % (name)
    buf = itf.read(name)
    # print "  --> '%s'" % (buf)

t1 = time.time()
print(
    "  Reading took {:.3f} s, or {:.3f} us / access."
    .format(t1 - t0, (t1 - t0) / niter * 1e6)
)


print("* Checking object integrity...")
nfail = 0
t0 = time.time()
for name in objmap:
    #print "Reading '%s'..." % (name)
    data = objmap[name]
    buf = itf.read(name)
    if buf != data:
        print("  Name = %s" % (name,))
        print("  Data = %s" % (data.enocde("ASCII"),))
        print("  Buf  = %s" % (buf.enocde("ASCII"),))
        nfail = nfail + 1

t1 = time.time()
print(
    "  Checking took {:.3f} s, or {:.3f} us / access. failcount = {}"
    .format(t1 - t0, (t1 - t0) / niter * 1e6, nfail)
)


print("* Checking object existence...")
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
    "  Existence checking took {:.3f} s, or {:.3f} us / access. "
    "count = {}, failcount = {}"
    .format(t1 - t0, (t1 - t0) / niter * 1e6, count, failcount)
)

print("* Checking for last entry")
lastobj = itf.last()
print("  Last added object is: {}".format(lastobj[0]))
if lastobj[0] != lastname:
    print("Name of last object is %s, last() returned %s. failcount = 1" \
          % (lastname,lastobj[0]))


print("* Closing file...")
itf.close()
print("Finished.")
