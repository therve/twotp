#!/usr/bin/env python
# Copyright (c) 2007-2008 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Example running erlang node.
"""

import sys

from twisted.python import log
from twisted.internet import reactor

from twotp import PersistentPortMapperFactory, String, Atom
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
    localPid = None

    def remote_get_cwd(self, proto, *args):
        from twisted.python import filepath
        cwd = filepath.FilePath(".").path
        return String(cwd)


    def remote_echo(self, proto, *args):
        return args[0]


    def onExit(self, *args):
        print "Got exit with message", args


    def remote_test_unlink(self, proto, pid):
        self.localPid.unlink(proto, pid)
        return ()


    def remote_test_link(self, proto, pid):
        if self.localPid is None:
            self.localPid = proto.factory.createPid()
        self.localPid.link(proto, pid)
        self.localPid.addExitHandler(pid, self.onExit)
        return pid


    def remote_get_pid(self, proto):
        if self.localPid is None:
            self.localPid = proto.factory.createPid()
        return self.localPid


    def remote_trigger_exit(self, proto, reason):
        reactor.callLater(3, self.localPid.exit, reason)
        return ()


    def remote_test_monitor(self, proto, pid):
        if self.localPid is None:
            self.localPid = proto.factory.createPid()
        ref = self.localPid.monitor(proto, pid)
        self.localPid.addMonitorHandler(ref, self.onExit)
        return ref


    def remote_test_demonitor(self, proto, pid, ref):
        self.localPid.demonitor(proto, pid, ref)
        return ()


    def remote_test_send(self, proto, pid):
        proto.factory.send(proto, pid, Atom("test"))
        return ()


    def remote_test_named_send(self, proto, name):
        if self.localPid is None:
            self.localPid = proto.factory.createPid()
        proto.factory.namedSend(proto, self.localPid, name, Atom("test"))
        return ()



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
