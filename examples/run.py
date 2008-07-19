#!/usr/bin/env python
# Copyright (c) 2007-2008 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Example running erlang node.
"""

import sys

from twisted.python import log
from twisted.internet import reactor

from twotp import PersistentPortMapperFactory, Tuple, Atom, String
from twotp import OneShotPortMapperFactory, readCookie, buildNodeName



def testPing(epmdFactory):
    def cb(inst):
        return inst.factory.ping(inst).addCallback(cb3)
    def cb3(resp):
        print "Got response", resp
    def eb(error):
        print "Got error", error
    epmdFactory.connectToNode("erlang").addCallback(cb).addErrback(eb)


class Proxy(object):
    def remote_get_cwd(self, *args):
        from twisted.python import filepath
        cwd = filepath.FilePath(".").path
        return Tuple((Atom("ok"), String(cwd)))

    def remote_echo(self, *args):
        return Tuple((Atom("ok"), args[0]))


def main1():
    cookie = readCookie()
    nodeName = buildNodeName(sys.argv[1])
    epmd = PersistentPortMapperFactory(nodeName, cookie)
    epmd.publish(file=Proxy())
    reactor.run()



def main2():
    cookie = readCookie()
    nodeName = buildNodeName(sys.argv[1])
    epmd = OneShotPortMapperFactory(nodeName, cookie)
    reactor.callWhenRunning(testPing, epmd)
    reactor.run()


if __name__ == "__main__":
    log.startLogging(sys.stdout)
    if sys.argv[2] == "client":
        main2()
    else:
        main1()
