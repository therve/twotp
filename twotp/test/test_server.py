# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Test server node.
"""

from twisted.internet.task import Clock
from twisted.internet.defer import Deferred
from twisted.test.proto_helpers import StringTransportWithDisconnection

from twotp.server import NodeServerProtocol, NodeServerFactory
from twotp.test.util import TestCase
from twotp.node import MessageHandler



class DummyServerFactory(object):
    """
    A dummy server factory for tests.
    """

    distrVersion = 5
    distrFlags = 268

    def __init__(self):
        """
        Initialize with testable values.
        """
        self.times = range(10)
        self.netTickTime = 30
        self.handler = MessageHandler("spam@egg", "test_cookie")


    def timeFactory(self):
        """
        Return predictable time.
        """
        return self.times.pop(0)


    def randomFactory(self):
        """
        Return always the same predictable number.
        """
        return 2



class NodeServerProtocolTestCase(TestCase):
    """
    Tests for L{NodeServerProtocol}.
    """

    def setUp(self):
        """
        Create an instance of C{NodeClientProtocol} and connect it to a fake
        transport.
        """
        self.factory = DummyServerFactory()
        self.proto = NodeServerProtocol()
        self.transport = StringTransportWithDisconnection()
        self.proto.factory = self.factory
        self.proto.makeConnection(self.transport)
        self.transport.protocol = self.proto
        self.clock = Clock()
        self.proto.callLater = self.clock.callLater


    def test_handshake(self):
        """
        Test a handshake data received.
        """
        remainData = self.proto.handle_handshake(
            "\x00\x0an\x00\x01\x00\x00\x00\x02foo")
        self.assertEquals(remainData, "")
        self.assertEquals(self.proto.state, "challenge")
        self.assertEquals(self.transport.value(),
            "\x00\x03sok"
            "\x00\x13n\x00\x05\x00\x00\x01\x0c\x00\x00\x00\x02spam@egg")


    def test_handshakeFragmented(self):
        """
        Test a handshake data received in fragments.
        """
        self.proto.dataReceived("\x00")
        self.proto.dataReceived("\x0a")
        self.proto.dataReceived("n\x00\x01\x00")
        self.proto.dataReceived("\x00\x00\x02foo")
        self.assertEquals(self.proto.state, "challenge")
        self.assertEquals(self.transport.value(),
            "\x00\x03sok"
            "\x00\x13n\x00\x05\x00\x00\x01\x0c\x00\x00\x00\x02spam@egg")


    def test_handshakeInvalidIdentifier(self):
        """
        Test a handshake with an invalid indentifier.
        """
        self.assertRaises(ValueError, self.proto.handle_handshake,
            "\x00\x0aN\x00\x01\x00\x00\x00\x02foo")


    def test_challenge(self):
        """
        Test a challenge data received.
        """
        self.proto.challenge = self.proto.generateChallenge()
        remainData = self.proto.handle_challenge(
            "\x00\x15r\x00\x00\x00\x05I\x14\xa6U'\xe0\x89\x14<\x1a\xdc\xf9(G&!")
        self.assertEquals(remainData, "")
        self.assertEquals(self.proto.state, "connected")
        self.assertEquals(self.transport.value(),
            "\x00\x11a\xe0\xdf2<\xe8\xbd\xa1o\xec\xe2\x12\xe5\x9c\xc6\xf7\x94")


    def test_challengeFragmented(self):
        """
        Test a challenge data received in fragments.
        """
        self.proto.challenge = self.proto.generateChallenge()
        self.proto.state = "challenge"
        self.proto.dataReceived("\x00")
        self.proto.dataReceived("\x15")
        self.proto.dataReceived("r\x00\x00\x00\x05I\x14\xa6U'\xe0")
        self.proto.dataReceived("\x89\x14<\x1a\xdc\xf9(G&!")
        self.assertEquals(self.proto.state, "connected")
        self.assertEquals(self.transport.value(),
            "\x00\x11a\xe0\xdf2<\xe8\xbd\xa1o\xec\xe2\x12\xe5\x9c\xc6\xf7\x94")


    def test_challengeInvalidIdentifier(self):
        """
        Test a challenge with an invalid indentifier.
        """
        self.proto.challenge = self.proto.generateChallenge()
        self.assertRaises(ValueError, self.proto.handle_challenge,
            "\x00\x15R\x00\x00\x00\x05SkH\x1f\xd8Z\xf0\"\xe2\xf5\xd6x2\xe9!\xe6")


    def test_challengeInvalidDigest(self):
        """
        Test a challenge with a wrong digest.
        """
        self.proto.challenge = self.proto.generateChallenge()
        self.assertRaises(ValueError, self.proto.handle_challenge,
            "\x00\x15r\x00\x00\x00\x05SkH\x1f\xd8Z\xf1\"\xe2\xf5\xd6x2\xe9!\xe6")



class NodeServerFactoryTestCase(TestCase):
    """
    Tests for C{NodeServerFactory}.
    """

    def test_epmdConnectDeferred(self):
        """
        Test for EPMD connection deferred: it should set the creation variable
        of the factory.
        """
        d = Deferred()
        factory = NodeServerFactory("foo@bar", "test_cookie", d)
        self.assertEquals(factory.creation, 0)
        d.callback(2)
        self.assertEquals(factory.creation, 2)


    def test_cache(self):
        """
        C{NodeServerFactory} keeps track of all connected instances once the
        challenge operation succeeds.
        """
        d = Deferred()
        factory = NodeServerFactory("foo@bar", "test_cookie", d)
        factory.randomFactory = lambda: 2
        proto = factory.buildProtocol(None)
        transport = StringTransportWithDisconnection()
        proto.makeConnection(transport)
        transport.protocol = proto
        clock = Clock()
        proto.callLater = clock.callLater

        proto.dataReceived(
            "\x00\x0an\x00\x01\x00\x00\x00\x02foo")
        proto.dataReceived(
            "\x00\x15r\x00\x00\x00\x05I\x14\xa6U'\xe0\x89\x14<\x1a\xdc\xf9(G&!")
        self.assertEquals(factory._nodeCache, {"foo": proto})
