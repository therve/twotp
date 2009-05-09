# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Test for EPMD connector.
"""

from twisted.internet.defer import Deferred
from twisted.internet.error import ConnectionDone
from twisted.internet import address
from twisted.test.proto_helpers import StringTransportWithDisconnection

from twotp.term import Node
from twotp.epmd import PortMapperProtocol, PersistentPortMapperFactory
from twotp.epmd import OneShotPortMapperFactory, NodeNotFound

from twotp.test.test_client import DummyClientFactory
from twotp.test.util import TestCase



class DummyPortMapperFactory(object):
    """
    Fake EPMD factory for tests.
    """

    def __init__(self):
        """
        Create a deferred to be fired later.
        """
        self._connectDeferred = Deferred()



class PortMapperProtocolTestCase(TestCase):
    """
    Tests for L{PortMapperProtocol}.
    """

    def setUp(self):
        """
        Create a protocol instance linked with a L{DummyPortMapperFactory}.
        """
        self.proto = PortMapperProtocol()
        self.proto.factory = DummyPortMapperFactory()
        self.transport = StringTransportWithDisconnection()
        self.proto.makeConnection(self.transport)
        self.transport.protocol = self.proto


    def test_alive2Response(self):
        """
        Test an successful alive2 response.
        """
        def cb(res):
            self.assertEquals(res, 1)
        self.proto.factory._connectDeferred.addCallback(cb)
        self.proto.dataReceived("y")
        self.proto.dataReceived("\x00")
        self.proto.dataReceived("\x00\x01")
        return self.proto.factory._connectDeferred


    def test_alive2ResponseError(self):
        """
        Test an erroneous alive2 response.
        """
        def eb(res):
            res.trap(ValueError)
        self.proto.factory._connectDeferred.addErrback(eb)
        self.proto.dataReceived("y\x01\x00\x01")
        return self.proto.factory._connectDeferred


    def test_alive2Request(self):
        """
        Test data sent by an alive2 request.
        """
        self.proto.alive2Request(1234, 77, (1, 2), "foo@bar")
        self.assertEquals(self.transport.value(),
            "\x00\x14x\x04\xd2M\x00\x00\x01\x00\x02\x00\x07foo@bar\x00\x00")


    def test_port2Response(self):
        """
        Test an successful port2 response.
        """
        d = Deferred()
        def cb(res):
            self.assertEquals(res, Node(9, 77, 1, (5, 5), "bar", ""))
        d.addCallback(cb)
        self.proto.deferred = d
        self.proto.dataReceived("w")
        self.proto.dataReceived("\x00")
        self.proto.dataReceived("\x00\x09M\x01\x00\x05\x00\x05\x00\x03bar\x00")
        self.assertEquals(self.proto.received, "")
        return d


    def test_port2ResponseWithExtra(self):
        """
        Test an successful port2 response, with extra data.
        """
        d = Deferred()
        def cb(res):
            self.assertEquals(res, Node(9, 77, 1, (5, 5), "bar", "spam"))
        d.addCallback(cb)
        self.proto.deferred = d
        self.proto.dataReceived(
            "w\x00\x00\x09M\x01\x00\x05\x00\x05\x00\x03bar\x00\x04spam")
        self.assertEquals(self.proto.received, "")
        return d


    def test_port2ResponseNodeNotFound(self):
        """
        Test a port2 not found response: the response deferred should be fired
        with a L{NodeNotFound} exception.
        """
        d = Deferred()
        self.proto.deferred = d
        self.proto.dataReceived("w\x01")
        self.assertEquals(self.proto.received, "")
        return self.assertFailure(d, NodeNotFound)


    def test_port2Request(self):
        """
        Test data sent by a port2 request.
        """
        d = Deferred()
        self.proto.portPlease2Request(d, "egg@spam")
        self.assertEquals(self.transport.value(), "\x00\tzegg@spam")
        self.assertEquals(self.proto.deferred, d)


    def test_names(self):
        """
        Test successful names request and response.
        """
        d = Deferred()
        def cb(res):
            self.assertEquals(res, [("foo", 1234), ("egg", 4321)])
        d.addCallback(cb)
        self.proto.namesRequest(d)
        self.assertEquals(self.transport.value(), "\x00\x01n")
        self.proto.dataReceived("\x00\x00\x00\x01")
        self.proto.dataReceived("name %s at port %s\n" % ("foo", 1234))
        self.proto.dataReceived("name %s at port %s\n" % ("egg", 4321))
        self.transport.loseConnection()
        return d


    def test_dump(self):
        """
        Test successful dump request and response.
        """
        d = Deferred()
        def cb(res):
            self.assertEquals(res, {"active": [("foo", 1234, 3)],
                                    "old": [("egg", 4321, 2)]})
        d.addCallback(cb)
        self.proto.dumpRequest(d)
        self.assertEquals(self.transport.value(), "\x00\x01d")
        self.proto.dataReceived("\x00\x00\x00\x01")
        self.proto.dataReceived(
            "active name    <%s> at port %s, fd = %s\n\x00" % ("foo", 1234, 3))
        self.proto.dataReceived(
            "old/unused name, <%s>, at port %s, fd = %s\n\x00" % ("egg",
                                                                  4321, 2))
        self.transport.loseConnection()
        return d


    def test_kill(self):
        """
        Test successful kill request and response.
        """
        d = Deferred()
        def cb(res):
            self.assertEquals(res, "OK")
        d.addCallback(cb)
        self.proto.killRequest(d)
        self.assertEquals(self.transport.value(), "\x00\x01k")
        self.proto.dataReceived("OK")
        self.transport.loseConnection()
        return d


    def test_invalidKillResponse(self):
        """
        Test a kill request with an invalid response.
        """
        d = Deferred()
        self.proto.killRequest(d)
        self.assertEquals(self.transport.value(), "\x00\x01k")
        self.proto.dataReceived("Wrong")
        self.transport.loseConnection()
        return self.assertFailure(d, ValueError)


    def test_connectionLostWithPendingDeferred(self):
        """
        Test that connectionLost errbacks the action deferred if present.
        """
        d = Deferred()
        self.proto.deferred = d
        self.transport.loseConnection()
        return self.assertFailure(d, ConnectionDone)


    def test_unhandledRequest(self):
        """
        When a unhandled data is received, the protocol should log a
        C{RuntimeError}.
        """
        self.proto.dataReceived("Wrong")
        errors = self.flushLoggedErrors()
        self.assertEquals(len(errors), 1)
        errors[0].trap(RuntimeError)



class DummyPort(object):
    """
    A dummy L{twisted.internet.tcp.Port} object.
    """

    def __init__(self, portNumber):
        """
        @param portNumber: the port which is used as listening address.
        @type portNumber: C{int}.
        """
        self.portNumber = portNumber


    def getHost(self):
        """
        Return a fake host.
        """
        return address.IPv4Address("TCP",
                *(("127.0.0.1", self.portNumber) + ("INET",)))



class TestablePPMF(PersistentPortMapperFactory):
    """
    A L{PersistentPortMapperFactory} without socket operations.
    """

    def __init__(self, nodeName, cookie):
        """
        Initialization: forward call and create attributes to save future
        calls.
        """
        PersistentPortMapperFactory.__init__(self, nodeName, cookie)
        self.connect = []
        self.listen = []


    def listenTCP(self, portNumber, factory):
        """
        A fake listenTCP.
        """
        self.listen.append((portNumber, factory))
        p = DummyPort(portNumber)
        return p


    def connectTCP(self, host, port, factory):
        """
        A fake connectTCP.
        """
        self.connect.append((host, port, factory))



class PersistentPortMapperFactoryTestCase(TestCase):
    """
    Tests for L{PersistentPortMapperFactory}.
    """

    def setUp(self):
        """
        Create a factory to be used in the tests.
        """
        self.factory = TestablePPMF("foo@bar", "test_cookie")


    def test_publish(self):
        """
        Test publish: this should create a server, and connect to the EPMD.
        """
        d = self.factory.publish()
        self.assertEquals(self.factory.listen[0][0], 0)
        self.assertIsInstance(self.factory.listen[0][1],
                              self.factory.nodeFactoryClass)
        self.assertEquals(self.factory.connect[0][:2], ("127.0.0.1", 4369))
        self.assertIdentical(self.factory.connect[0][2], self.factory)
        self.factory._connectDeferred.callback(2)
        def cb(ign):
            self.assertEquals(self.factory.listen[0][1].creation, 2)
        return d.addCallback(cb)


    def test_connection(self):
        """
        On connection, the EPMD client should send an ALIVE2 request.
        """
        self.factory.nodePortNumber = 1234
        transport = StringTransportWithDisconnection()
        proto = self.factory.buildProtocol(("127.0.01", 4369))
        proto.makeConnection(transport)
        transport.protocol = proto
        self.assertEquals(transport.value(),
            "\x00\x10x\x04\xd2H\x00\x00\x05\x00\x05\x00\x03foo\x00\x00")



class TestableOSPMF(OneShotPortMapperFactory):
    """
    A L{OneShotPortMapperFactory} without socket operations.
    """

    def __init__(self, nodeName, cookie, host="127.0.0.1"):
        """
        Initialization: forward call and create attributes to save future
        calls.
        """
        OneShotPortMapperFactory.__init__(self, nodeName, cookie, host=host)
        self.connect = []


    def connectTCP(self, host, port, factory):
        """
        A fake connectTCP.
        """
        self.connect.append((host, port, factory))



class OneShotPortMapperFactoryTestCase(TestCase):
    """
    Tests for L{OneShotPortMapperFactory}
    """

    def setUp(self):
        """
        Create a factory to be used in the tests.
        """
        self.factory = TestableOSPMF("foo@bar", "test_cookie")


    def test_portPlease2(self):
        """
        L{OneShotPortMapperFactory.portPlease2Request} should create a
        connection to a new host, and set its initial action to
        B{portPlease2Request} with the nodeName given.
        """
        d = self.factory.portPlease2Request("egg@spam")
        transport = StringTransportWithDisconnection()
        proto = self.factory.buildProtocol(("127.0.01", 4369))
        proto.makeConnection(transport)
        transport.protocol = proto
        self.assertEquals(transport.value(), "\x00\tzegg@spam")
        self.assertEquals(self.factory.connect,
            [("127.0.0.1", 4369, self.factory)])
        proto.dataReceived(
                "w\x00\x00\x09M\x01\x00\x05\x00\x05\x00\x03bar\x00")
        return d.addCallback(self.assertEquals,
                             Node(9, 77, 1, (5, 5), "bar", ""))


    def test_connectToNode(self):
        """
        L{OneShotPortMapperFactory.connectToNode} should return a L{Deferred}
        that will be called back with the instance of the protocol connected
        to the erlang node.
        """
        clientFactory = DummyClientFactory()
        self.factory.nodeFactoryClass = lambda a, b, c: clientFactory
        d = self.factory.connectToNode("egg@spam")
        transport = StringTransportWithDisconnection()
        proto = self.factory.buildProtocol(("127.0.01", 4369))
        proto.makeConnection(transport)
        transport.protocol = proto
        self.assertEquals(transport.value(), "\x00\x04zegg")
        self.assertEquals(self.factory.connect,
            [("127.0.0.1", 4369, self.factory)])
        proto.dataReceived(
            "w\x00\x00\x09M\x01\x00\x05\x00\x05\x00\x03bar\x00")
        clientProto = object()
        clientFactory._connectDeferred.callback(clientProto)
        self.assertEquals(self.factory.connect,
            [("127.0.0.1", 4369, self.factory),
             ("127.0.0.1", 9, clientFactory)])
        return d.addCallback(self.assertIdentical, clientProto)


    def test_connectToNodeInCache(self):
        """
        If a previous client is present in the cache of the factory,
        L{OneShotPortMapperFactory.connectToNode} should directly return it.
        """
        clientProto = object()
        self.factory._nodeCache["egg"] = clientProto
        d = self.factory.connectToNode("egg@spam")
        return d.addCallback(self.assertIdentical, clientProto)


    def test_connectionLostCleanCache(self):
        """
        When a connectionLost is forwarded from a client factory, it should
        remove the client from the cache.
        """
        clientProto1 = object()
        clientProto2 = object()
        self.factory._nodeCache["spam@egg"] = clientProto1
        self.factory._nodeCache["foo@bar"] = clientProto2
        clientFactory = DummyClientFactory()
        self.factory.onConnectionLost(clientFactory)
        self.assertEquals(self.factory._nodeCache, {"foo@bar": clientProto2})


    def test_names(self):
        """
        L{OneShotPortMapperFactory.names} should return a L{Deferred} that
        will be called back when the names request is complete.
        """
        d = self.factory.names()
        transport = StringTransportWithDisconnection()
        proto = self.factory.buildProtocol(("127.0.01", 4369))
        proto.makeConnection(transport)
        transport.protocol = proto
        self.assertEquals(transport.value(), "\x00\x01n")
        proto.dataReceived("\x00\x00\x00\x01")
        proto.dataReceived("name %s at port %s\n" % ("foo", 1234))
        transport.loseConnection()
        return d.addCallback(self.assertEquals, [("foo", 1234)])


    def test_dump(self):
        """
        L{OneShotPortMapperFactory.dump} should return a L{Deferred} that
        will be called back when the dump request is complete.
        """
        d = self.factory.dump()
        transport = StringTransportWithDisconnection()
        proto = self.factory.buildProtocol(("127.0.01", 4369))
        proto.makeConnection(transport)
        transport.protocol = proto
        self.assertEquals(transport.value(), "\x00\x01d")
        proto.dataReceived("\x00\x00\x00\x01")
        proto.dataReceived(
            "active name    <%s> at port %s, fd = %s\n\x00" % ("foo", 1234, 3))
        transport.loseConnection()
        return d.addCallback(self.assertEquals,
                {"active": [("foo", 1234, 3)], "old": []})


    def test_kill(self):
        """
        L{OneShotPortMapperFactory.kill} should return a L{Deferred} that
        will be called back when the kill request is complete.
        """
        d = self.factory.kill()
        transport = StringTransportWithDisconnection()
        proto = self.factory.buildProtocol(("127.0.01", 4369))
        proto.makeConnection(transport)
        transport.protocol = proto
        self.assertEquals(transport.value(), "\x00\x01k")
        proto.dataReceived("OK")
        transport.loseConnection()
        return d.addCallback(self.assertEquals, "OK")

