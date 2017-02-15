#!/usr/bin/env python2
import socket
import numpy as np
from numpy import uint8, uint16, uint32
import time

def GetCheckSum(buf, key):
    buf = np.array([buf], dtype='S').view('u1')

    crc = uint16(0)
    for i in range(len(buf)):
        crc = DoCRC(crc, buf[i])

    lword, irword = PSDES(uint32(crc), uint32(key))
    return "{:08x}{:08x}".format(lword, irword)

def DoCRC(crc, onech):
    assert(type(crc) is uint16 and type(onech) is uint8)

    ans = uint32(crc ^ onech << uint16(8))
    for ind in range(8):
        if ans & uint32(0x8000):
          ans <<= uint32(1)
          ans = ans ^ uint32(4129)
        else:
          ans <<= uint32(1)
    return uint16(ans)

def PSDES(lword, irword):
    assert(type(lword) is uint32 and type(irword) is uint32)

    c1 = [uint32(x) for x in [0xcba4e531, 0x537158eb, 0x145cdc3c, 0x0d3fdeb2]]
    c2 = [uint32(x) for x in [0x12be4590, 0xab54ce58, 0x6954c7a6, 0x15a2ca46]]

    for index in range(4):
        iswap = irword
        ia = irword ^ c1[index]

        itmpl = ia & uint32(0xffff)
        itmph = ia >> uint32(16)

        ib = (itmpl * itmpl) + ~(itmph*itmph)
        ia = (ib >> uint32(16)) | ((ib & uint32(0xffff)) << uint32(16))

        irword = (lword) ^ ((ia ^ c2[index]) + (itmpl * itmph))
        lword = iswap

    return lword, irword

def getHeader(command, auth, key):
    msg = "TS={} AUTH={} DT={}".format(int(time.time()), auth, command)
    checksum = GetCheckSum(msg, key)
    request = "CK={} ".format(checksum) + msg
    
    return "{:08d}\n{}".format(len(request), request)


def showq(username):
    HOST = '129.32.84.162'    # The remote host
    PORT = 42559              # The same port as used by the server
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    
    key = 0
    command = "CMD=showq AUTH={} ARG=1 ALL 0 \n".format(username)
    header = getHeader(command, username, key)
    print repr(header)

    s.sendall(header)
    size = int(s.recv(9))
    data = s.recv(size)
    s.close()

    return data

def qstat(username):
    HOST = '129.32.84.162'    # The remote host
    PORT = 42559              # The same port as used by the server
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    
    key = 0
    command = "CMD=qstat AUTH={} ARG=1 ALL 0 \n".format(username)
    header = getHeader(command, username, key)
    print repr(header)

    s.sendall(header)
    size = int(s.recv(9))
    data = s.recv(size)
    s.close()

    return data

def checkjob(username, jobid):
    HOST = '129.32.84.162'    # The remote host
    PORT = 42559              # The same port as used by the server
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    
    key = 0
    command = "CMD=checkjob AUTH={} ARG=2 {} {} 0 \n".format(username, jobid,
            jobid)
    header = getHeader(command, username, key)
    print repr(header)

    s.sendall(header)
    size = int(s.recv(9))
    data = s.recv(size)
    s.close()

    return data

if __name__ == '__main__':
    username = 'tuf33565'
    jobid = 29658
    #print showq(username)
    for i in range(10):
        print checkjob(username, jobid)
    #print qstat(username)
