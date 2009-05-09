import sys

from twisted.python import log
from twisted.internet import reactor

from twotp import Process, readCookie, buildNodeName, Atom



def testPing(process):
    def cb(resp):
        print "Got response", resp
        process.register("bar")
        return process.receive().addCallback(cb).addErrback(eb)
    def eb(error):
        print "Got error", error
    return process.ping("erlang").addCallback(cb).addErrback(eb)


def testReceive(process):
    def cb2(resp):
        print "Got response", resp
    def cb(resp):
        print "Got response", resp
        #return process.receive().addCallback(cb).addErrback(eb)
        return process.ping("erlang").addCallback(cb2).addErrback(eb)
    def eb(error):
        print "Got error", error
    #return process.whereis("erlang", "foo").addCallback(cb).addErrback(eb)
    #return process.namedSend("erlang", "foo", 3).addCallback(cb).addErrback(eb)
    process.register("bar")
    return process.receive().addCallback(cb).addErrback(eb)


class RemoteCalls(object):

    def __init__(self, process):
        self.process = process

    def remote_get_pid(self, proto):
        return self.process.pid


def main(nodeName):
    cookie = readCookie()
    nodeName = buildNodeName(nodeName)
    process = Process(nodeName, cookie)
    process.listen(twisted=RemoteCalls(process)
        ).addCallback(lambda x: testReceive(process))
    #testPing(process)
    reactor.run()


if __name__ == "__main__":
    log.startLogging(sys.stdout)
    main(sys.argv[1])
