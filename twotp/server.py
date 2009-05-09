# -*- test-case-name: twotp.test.test_server -*-
# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Server node protocol.
"""

import struct

from twisted.internet.protocol import ServerFactory
from twisted.internet.defer import Deferred
from twisted.python import log

from twotp.node import NodeProtocol, NodeBaseFactory, InvalidIdentifier, InvalidDigest



class NodeServerProtocol(NodeProtocol):
    """
    @ivar state: 'handshake', 'challenge', 'connected'.
    @type state: C{str}
    """
    _connectDeferred = None


    def connectionMade(self):
        """
        Called when a connection is made from an erlang node.
        """
        log.msg("Node server connection")


    def handle_handshake(self, data):
        """
        Handle data during the handshake state.
        """
        if len(data) < 2:
            return data
        parser = self.factory.handler._parser
        packetLen = parser.parseShort(data[0:2])
        if len(data) < packetLen + 2:
            return data
        packetData = data[2:packetLen+2]
        if packetData[0] != "n":
            raise InvalidIdentifier("Got %r instead of 'n'" % (packetData[0],))
        self.peerVersion = parser.parseShort(packetData[1:3])
        self.peerFlags = parser.parseInt(packetData[3:7])
        self.peerName = packetData[7:]
        self.send("sok")
        self.sendChallenge()
        self.state = "challenge"
        return data[packetLen + 2:]


    def handle_challenge(self, data):
        """
        Handle data during the challenge state.
        """
        if len(data) < 2:
            return data
        packetLen = self.factory.handler._parser.parseShort(data[0:2])
        if len(data) < packetLen + 2:
            return data
        packetData = data[2:packetLen+2]
        if packetData[0] != "r":
            raise InvalidIdentifier("Got %r instead of 'r'" % (packetData[0],))
        peerChallenge = self.factory.handler._parser.parseInt(packetData[1:5])
        peerDigest = packetData[5:]
        ownDigest = self.generateDigest(
            self.challenge, self.factory.handler.cookie)
        if peerDigest != ownDigest:
            raise InvalidDigest("Digest doesn't match, node disallowed")
        self.sendAck(peerChallenge)
        self.state = "connected"
        if self._connectDeferred is not None:
            d, self._connectDeferred = self._connectDeferred, None
            d.callback(self)
        self.startTimer()
        return data[packetLen + 2:]


    def sendChallenge(self):
        """
        Send initial challenge.
        """
        self.challenge = self.generateChallenge()
        handler = self.factory.handler
        flags = struct.pack(
            "!HII", handler.distrVersion, handler.distrFlags, self.challenge)
        msg = "n%s%s" % (flags, handler.nodeName)
        self.send(msg)


    def sendAck(self, challenge):
        """
        Send final ack after challenge check.
        """
        msg = "a" + self.generateDigest(challenge, self.factory.handler.cookie)
        self.send(msg)



class NodeServerFactory(NodeBaseFactory, ServerFactory):
    """
    Server factory for C{NodeServerProtocol}.
    """
    protocol = NodeServerProtocol

    def __init__(self, nodeName, cookie, epmdConnectDeferred):
        """
        Initialize the server factory.
        """
        NodeBaseFactory.__init__(self, nodeName, cookie)
        epmdConnectDeferred.addCallback(self._epmdConnected)
        self._nodeCache = {}


    def _epmdConnected(self, creation):
        """
        Callback fired when connected to the EPMD.
        """
        self.creation = creation
        return self


    def _putInCache(self, instance):
        """
        Store the connection in a cache for being used later on.
        """
        self._nodeCache[instance.peerName] = instance


    def buildProtocol(self, addr):
        """
        Build the L{NodeServerProtocol} instance and add a callback when the
        connection has been successfully established to the other node.
        """
        p = ServerFactory.buildProtocol(self, addr)
        p._connectDeferred = Deferred()
        p._connectDeferred.addCallback(self._putInCache)
        return p
