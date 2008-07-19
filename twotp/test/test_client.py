# Copyright (c) 2007-2008 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Test client node.
"""

from twisted.internet.task import Clock
from twisted.internet.defer import Deferred
from twisted.internet.error import ConnectionDone
from twisted.test.proto_helpers import StringTransportWithDisconnection

from twotp.client import NodeClientProtocol, NodeClientFactory
from twotp.test.util import TestCase



class CloseNotifiedTransport(StringTransportWithDisconnection):
    """
    A string transport that saves its closed state.
    """
    closed = False

    def loseConnection(self):
        """
        Save the connection lost state, and forward the call.
        """
        StringTransportWithDisconnection.loseConnection(self)
        self.closed = True



class DummyClientFactory(object):
    """
    A dummy client factory for tests.
    """

    def __init__(self):
        """
        Initialize with testable values.
        """
        self.times = range(10)
        self.nodeName = "spam@egg"
        self.cookie = "test_cookie"
        self.netTickTime = 30
        self._connectDeferred = Deferred()


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



class NodeClientProtocolTestCase(TestCase):
    """
    Tests for L{NodeClientProtocol}.
    """

    def setUp(self):
        """
        Create an instance of C{NodeClientProtocol} and connect it to a fake
        transport.
        """
        self.factory = DummyClientFactory()
        self.proto = NodeClientProtocol()
        self.transport = CloseNotifiedTransport()
        self.proto.factory = self.factory
        self.proto.makeConnection(self.transport)
        self.transport.protocol = self.proto
        self.clock = Clock()
        self.proto.callLater = self.clock.callLater
        self.assertEquals(self.transport.value(),
            "\x00\x0fn\x00\x05\x00\x00\x01\x04spam@egg")
        self.transport.clear()


    def test_handshake(self):
        """
        Test an OK handshake received.
        """
        remainData = self.proto.handle_handshake("\x00\x03sok")
        self.assertEquals(remainData, "")
        self.assertEquals(self.proto.state, "challenge")

        remainData = self.proto.handle_handshake("\x00\x10sok_simultaneous")
        self.assertEquals(remainData, "")
        self.assertEquals(self.proto.state, "challenge")


    def test_handshakeFragmented(self):
        """
        Test an OK handshake received in fragments.
        """
        self.proto.dataReceived("\x00")
        self.proto.dataReceived("\x03")
        self.proto.dataReceived("sok")
        self.assertEquals(self.proto.state, "challenge")


    def test_handshakeAlive(self):
        """
        Test an alive handshake received.
        """
        remainData = self.proto.handle_handshake("\x00\x06salive")
        self.assertEquals(remainData, "")
        self.assertEquals(self.proto.state, "challenge")
        self.assertEquals(self.transport.value(), "\x00\x04true")


    def test_handshakeRefused(self):
        """
        Test a refused handshake.
        """
        d = self.assertFailure(self.factory._connectDeferred, ValueError)
        remainData = self.proto.handle_handshake("\x00\x04snok")
        self.assertEquals(remainData, "")
        self.assertTrue(self.transport.closed)
        return d


    def test_handshakeNotAllowed(self):
        """
        Test a refused handshake, with reason 'not_allowed'.
        """
        d = self.assertFailure(self.factory._connectDeferred, ValueError)
        remainData = self.proto.handle_handshake("\x00\x0csnot_allowed")
        self.assertEquals(remainData, "")
        self.assertTrue(self.transport.closed)
        return d


    def test_invalidHandshake(self):
        """
        Check that an invalid status message close the connection.
        """
        d = self.assertFailure(self.factory._connectDeferred, ValueError)
        remainData = self.proto.handle_handshake("\x00\x04sfoo")
        self.assertEquals(remainData, "")
        self.assertTrue(self.transport.closed)
        return d


    def test_invalidHandshakeIdentifier(self):
        """
        Test an OK handshake received, but with an invalid 't' identifier: that
        should close the connection
        """
        d = self.assertFailure(self.factory._connectDeferred, ValueError)
        remainData = self.proto.handle_handshake("\x00\x03tok")
        self.assertEquals(remainData, "")
        self.assertTrue(self.transport.closed)
        return d


    def test_challengeOK(self):
        """
        Test challenge reading and its response.
        """
        remainData = self.proto.handle_challenge(
            "\x00\x12n\x00\x01\x00\x00\x00\x02\x00\x00\x00\x01foo@bar")
        self.assertEquals(remainData, "")
        self.assertEquals(self.transport.value(),
            "\x00\x15r\x00\x00\x00\x02\xd2\xb0'\xc0*\xfd\xebl\xa7yaM\xff#\x08\xce")
        self.assertEquals(self.proto.peerName, "foo@bar")
        self.assertEquals(self.proto.peerVersion, 1)
        self.assertEquals(self.proto.peerFlags, 2)
        self.assertEquals(self.proto.state, "challenge_ack")


    def test_challengeWrongIdentifier(self):
        """
        An invalid identifier should close the connection.
        """
        d = self.assertFailure(self.factory._connectDeferred, ValueError)
        remainData = self.proto.handle_challenge(
            "\x00\x12z\x00\x01\x00\x00\x00\x02\x00\x00\x00\x01foo@bar")
        self.assertEquals(remainData, "")
        self.assertEquals(self.transport.value(), "")
        self.assertTrue(self.transport.closed)
        return d


    def test_challengeFragmented(self):
        """
        Test a challenge received in fragments.
        """
        self.proto.state = "challenge"
        self.proto.dataReceived("\x00")
        self.proto.dataReceived("\x12")
        self.proto.dataReceived("n\x00\x01\x00\x00\x00\x02")
        self.proto.dataReceived("\x00\x00\x00\x01foo@bar")
        self.assertEquals(self.transport.value(),
            "\x00\x15r\x00\x00\x00\x02\xd2\xb0'\xc0*\xfd\xebl\xa7yaM\xff#\x08\xce")
        self.assertEquals(self.proto.peerName, "foo@bar")
        self.assertEquals(self.proto.peerVersion, 1)
        self.assertEquals(self.proto.peerFlags, 2)
        self.assertEquals(self.proto.state, "challenge_ack")
        self.assertEquals(self.proto.challenge, 2)


    def test_challengeAck(self):
        """
        Test reception of challenge ack.
        """
        self.proto.challenge = 4294967294
        self.factory._connectDeferred.addCallback(
            self.assertIdentical, self.proto)
        remainData = self.proto.handle_challenge_ack(
            "\x00\x11aSkH\x1f\xd8Z\xf0\"\xe2\xf5\xd6x2\xe9!\xe6")
        self.assertEquals(remainData, "")
        self.assertEquals(self.transport.value(), "")
        self.assertEquals(self.proto.state, "connected")
        return self.factory._connectDeferred


    def test_challengeAckWrongIdentifier(self):
        """
        Test reception of challenge ack.
        """
        d = self.assertFailure(self.factory._connectDeferred, ValueError)
        self.proto.challenge = 4294967294
        remainData = self.proto.handle_challenge_ack(
            "\x00\x11ASkH\x1f\xd8Z\xf0\"\xe2\xf5\xd6x2\xe9!\xe6")
        self.assertEquals(remainData, "")
        self.assertTrue(self.transport.closed)
        return d


    def test_challengeAckFragmented(self):
        """
        Test reception of challenge ack in framents.
        """
        self.proto.challenge = 4294967294
        self.proto.state = 'challenge_ack'
        self.factory._connectDeferred.addCallback(
            self.assertIdentical, self.proto)
        self.proto.dataReceived("\x00")
        self.proto.dataReceived("\x11aSkH\x1f")
        self.proto.dataReceived("\xd8Z\xf0\"\xe2\xf5\xd6x2\xe9!\xe6")
        self.assertEquals(self.transport.value(), "")
        self.assertEquals(self.proto.state, "connected")
        return self.factory._connectDeferred


    def test_challengeAckWrongDigest(self):
        """
        Check the reception of a wrong digest.
        """
        d = self.assertFailure(self.factory._connectDeferred, ValueError)
        self.proto.challenge = 4294967294
        remainData = self.proto.handle_challenge_ack(
            "\x00\x11aSkH\x1f\xd8Z\xf0\"\xe2\xf5\xd6x2\xe9!\xe5")
        self.assertEquals(remainData, "")
        self.assertTrue(self.transport.closed)
        return d


    def test_connectionLostDuringHandShare(self):
        """
        """
        self.proto.dataReceived("\x00\x03sok")
        self.transport.loseConnection()
        return self.assertFailure(self.factory._connectDeferred,
                                  ConnectionDone)



class NodeClientFactoryTestCase(TestCase):
    """
    Tests for C{NodeClientFactory}.
    """

    def test_forwardConnectionLost(self):
        """
        On connectionLost, the factory should notify the method passed in the
        constructor.
        """
        d = Deferred()
        factory = NodeClientFactory(None, "foo@bar", "cookie", d.callback)
        def cb(res):
            self.assertEquals(res, factory)
        d.addCallback(cb)
        factory.clientConnectionLost(None, None)
        return d

