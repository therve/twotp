#!/usr/bin/env python
# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Example running erlang node.
"""

import sys

from twisted.python import log
from twisted.internet import reactor

from twotp import Process, readCookie, buildNodeName



def testPing(process):
    def cb(resp):
        print "Got response", resp
    def eb(error):
        print "Got error", error
    return process.ping("erlang").addCallback(cb).addErrback(eb)


def testReceive(process):
    def cb(resp):
        print "Got response", resp
    def eb(error):
        print "Got error", error
    process.register("main")
    return process.receive().addCallback(cb).addErrback(eb)


class RemoteCalls(object):

    def __init__(self, process):
        self.process = process

    def remote_get_pid(self):
        return self.process.pid

    def remote_get_cwd(self, *args):
        from twisted.python import filepath
        return filepath.FilePath(".").path

    def remote_echo(self, *args):
        return args[0]


def main(nodeName, server=False):
    cookie = readCookie()
    nodeName = buildNodeName(nodeName)
    process = Process(nodeName, cookie)
    process.registerModule("api", RemoteCalls(process))
    if server:
        process.listen().addCallback(lambda x: testReceive(process))
    else:
        testPing(process)
    reactor.run()


if __name__ == "__main__":
    log.startLogging(sys.stdout)
    main(sys.argv[1], sys.argv[2] == "server")
