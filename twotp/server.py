# -*- test-case-name: twotp.test.test_server -*-
# Copyright (c) 2007-2008 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Server node protocol.
"""

import struct

from twisted.internet.protocol import ServerFactory
from twisted.python import log

from twotp.node import NodeProtocol, NodeBaseFactory, InvalidIdentifier, InvalidDigest
from twotp.parser import theParser



class NodeServerProtocol(NodeProtocol):
    """
    @ivar state: 'handshake', 'challenge', 'connected'.
    @type state: C{str}
    """

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
        packetLen = theParser.parseShort(data[0:2])
        if len(data) < packetLen + 2:
            return data
        packetData = data[2:packetLen+2]
        if packetData[0] != "n":
            raise InvalidIdentifier("Got %r instead of 'n'" % (packetData[0],))
        self.peerVersion = theParser.parseShort(packetData[1:3])
        self.peerFlags = theParser.parseInt(packetData[3:7])
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
        packetLen = theParser.parseShort(data[0:2])
        if len(data) < packetLen + 2:
            return data
        packetData = data[2:packetLen+2]
        if packetData[0] != "r":
            raise InvalidIdentifier("Got %r instead of 'r'" % (packetData[0],))
        peerChallenge = theParser.parseInt(packetData[1:5])
        peerDigest = packetData[5:]
        ownDigest = self.generateDigest(self.challenge, self.factory.cookie)
        if peerDigest != ownDigest:
            raise InvalidDigest("Digest doesn't match, node disallowed")
        self.sendAck(peerChallenge)
        self.state = "connected"
        self.startTimer()
        return data[packetLen + 2:]


    def sendChallenge(self):
        """
        Send initial challenge.
        """
        self.challenge = self.generateChallenge()
        msg = "n" + struct.pack("!HII", self.distrVersion, self.distrFlags,
                                self.challenge) + self.factory.nodeName
        self.send(msg)


    def sendAck(self, challenge):
        """
        Send final ack after challenge check.
        """
        msg = "a" + self.generateDigest(challenge, self.factory.cookie)
        self.send(msg)



class NodeServerFactory(NodeBaseFactory, ServerFactory):
    """
    Server factory for C{NodeServerProtocol}.
    """
    protocol = NodeServerProtocol

    def __init__(self, methodsHolder, nodeName, cookie, epmdConnectDeferred):
        """
        Initialize the server factory.
        """
        NodeBaseFactory.__init__(self, methodsHolder, nodeName, cookie)
        epmdConnectDeferred.addCallback(self._epmdConnected)


    def _epmdConnected(self, creation):
        """
        Callback fired when connected to the EPMD.
        """
        self.creation = creation


