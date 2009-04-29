"""
Example emulating rabbitmqctl, just calling list_vhosts for now.
"""

import sys

from twisted.python import log
from twisted.internet import reactor

from twotp import OneShotPortMapperFactory, readCookie, buildNodeName



def testListVhost(epmdFactory):
    def cb(inst):
        return inst.factory.callRemote(inst, "rabbit_access_control",
        "list_vhosts").addCallback(cb3)
    def cb3(resp):
        print "Got response", resp
    def eb(error):
        print "Got error", error
    epmdFactory.connectToNode("rabbit").addCallback(cb).addErrback(eb)



if __name__ == "__main__":
    log.startLogging(sys.stdout)
    cookie = readCookie()
    nodeName = buildNodeName("twotp-rabbit")
    epmd = OneShotPortMapperFactory(nodeName, cookie)
    reactor.callWhenRunning(testListVhost, epmd)
    reactor.run()

