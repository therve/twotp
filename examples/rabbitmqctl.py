# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Example emulating rabbitmqctl, just calling list_vhosts for now.
"""

from twisted.internet import reactor

from twotp import Process, readCookie, buildNodeName


def testListVhost(process):
    def cb(resp):
        print resp
        reactor.stop()
    def eb(error):
        print "Got error", error
        reactor.stop()
    process.callRemote("rabbit", "rabbit_access_control", "list_vhosts"
        ).addCallback(cb).addErrback(eb)


if __name__ == "__main__":
    cookie = readCookie()
    nodeName = buildNodeName("twotp-rabbit")
    process = Process(nodeName, cookie)
    reactor.callWhenRunning(testListVhost, process)
    reactor.run()
