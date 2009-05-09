# -*- test-case-name: twotp.test.test_client -*-
# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Client node protocol.
"""

import struct

from twisted.python import log
from twisted.internet.protocol import ClientFactory
from twisted.internet.defer import Deferred

from twotp.node import NodeProtocol, NodeBaseFactory, InvalidIdentifier, InvalidDigest
from twotp.packer import thePacker



class HandshakeRefused(ValueError):
    """
    Exception raised when handshake was refused by other end.
    """



class UnknownStatus(ValueError):
    """
    Exception raised upon reception of a unknown status in handshake.
    """



class NodeClientProtocol(NodeProtocol):
    """
    @ivar state: 'handshake', 'challenge', 'challenge_ack', 'connected'.
    @type state: C{str}
    """
    peerVersion = None
    peerFlags = None
    peerName = None

    def handle_handshake(self, data):
        """
        Handle data in the handshake state.
        """
        if len(data) < 2:
            return data
        packetLen = self.factory.handler._parser.parseShort(data[0:2])
        if len(data) < packetLen + 2:
            return data
        packetData = data[2:packetLen+2]
        if packetData[0] != "s":
            self.notifyFailure(
                InvalidIdentifier("Got %r instead of 's'" % (packetData[0],)))
            return ""
        status = packetData[1:]
        if status == "ok" or status == "ok_simultaneous":
            self.state = "challenge"
        elif status == "alive":
            self.sendAliveTrue()
            self.state = "challenge"
        elif status == "nok" or status == "not_allowed":
            self.notifyFailure(HandshakeRefused(status))
            return ""
        else:
            self.notifyFailure(UnknownStatus(status))
        return data[packetLen + 2:]


    def handle_challenge(self, data):
        """
        Handle data in the challenge state.
        """
        if len(data) < 2:
            return data
        packetLen = self.factory.handler._parser.parseShort(data[0:2])
        if len(data) < packetLen + 2:
            return data
        packetData = data[2:packetLen+2]
        if packetData[0] != "n":
            self.notifyFailure(
                InvalidIdentifier("Got %r instead of 'n'" % (packetData[0],)))
            return ""
        self.peerVersion, self.peerFlags, challenge = struct.unpack("!HII",
                                                          packetData[1:11])
        self.peerName = packetData[11:]

        self.sendChallengeReply(challenge)
        self.state = "challenge_ack"
        return data[packetLen + 2:]


    def handle_challenge_ack(self, data):
        """
        Handle data in the challenge_ack state.
        """
        if len(data) < 2:
            return data
        packetLen = self.factory.handler._parser.parseShort(data[0:2])
        if len(data) < packetLen + 2:
            return data
        packetData = data[2:packetLen+2]
        if packetData[0] != "a":
            self.notifyFailure(
                InvalidIdentifier("Got %r instead of 'a'" % (packetData[0],)))
            return ""
        peerDigest = packetData[1:]
        ownDigest = self.generateDigest(
            self.challenge, self.factory.handler.cookie)
        if peerDigest != ownDigest:
            self.notifyFailure(
                InvalidDigest("Digest doesn't match, node disallowed"))
            return ""
        self.state = "connected"
        if self.factory._connectDeferred is not None:
            d, self.factory._connectDeferred = self.factory._connectDeferred, None
            d.callback(self)
        self.startTimer()
        return data[packetLen + 2:]


    def sendAliveTrue(self):
        """
        Send message response for alive request.
        """
        self.send("true")


    def sendName(self):
        """
        Send name for introduction message.
        """
        handler = self.factory.handler
        flags = struct.pack("!HI", handler.distrVersion, handler.distrFlags)
        msg = "n%s%s" % (flags, handler.nodeName)
        self.send(msg)


    def sendChallengeReply(self, challenge):
        """
        Send reply to challenge.
        """
        digest = self.generateDigest(challenge, self.factory.handler.cookie)
        self.challenge = self.generateChallenge()
        msg = "r" + thePacker.packInt(self.challenge) + digest
        self.send(msg)


    def connectionMade(self):
        """
        Send initial message at connection.
        """
        log.msg("Node client connection")
        self.sendName()


    def notifyFailure(self, reason):
        """
        Forward result to factory if necessary, and close the connection.
        """
        if self.factory._connectDeferred is not None:
            d, self.factory._connectDeferred = self.factory._connectDeferred, None
            d.errback(reason)
        self.transport.loseConnection()


    def connectionLost(self, reason):
        """
        Upon close, notify factory.
        """
        NodeProtocol.connectionLost(self, reason)
        if self.factory._connectDeferred is not None:
            self.factory._connectDeferred.errback(reason)



class NodeClientFactory(NodeBaseFactory, ClientFactory):
    """
    Factory for L{NodeClientProtocol}.
    """
    protocol = NodeClientProtocol

    def __init__(self, nodeName, cookie, onConnectionLost=lambda x: None):
        """
        Initialize the server factory.
        """
        NodeBaseFactory.__init__(self, nodeName, cookie)
        self._connectDeferred = Deferred()
        self.onConnectionLost = onConnectionLost


    def clientConnectionLost(self, connector, reason):
        """
        Forward call to notify EPMD client.
        """
        self.onConnectionLost(self)
