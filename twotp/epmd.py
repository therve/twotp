# -*- test-case-name: twotp.test.test_epmd -*-
# Copyright (c) 2007-2008 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Erlang Port Mapper Daemon client code.
"""

import struct

from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet.defer import Deferred, succeed
from twisted.python import log
from twisted.application.service import Service
from twisted.application.internet import TCPClient, TCPServer

from twotp.parser import theParser
from twotp.packer import thePacker
from twotp.term import Node
from twotp.client import NodeClientFactory
from twotp.server import NodeServerFactory



class NodeNotFound(Exception):
    """
    Exception raised when a node is not found by the EPMD.
    """



class PortMapperProtocol(Protocol):
    """
    EPMD (Erlang Port Mapper Daemon) protocol.
    """
    ALIVE2_REQ = 120
    PORT_PLEASE2_REQ = 122
    NAMES_REQ = 110
    DUMP_REQ = 100
    KILL_REQ = 107
    STOP_REQ = 115
    ALIVE2_RESP = 121
    PORT2_RESP = 119

    initialAction = None
    deferred = None
    endDataReceived = None

    def __init__(self):
        """
        Initialize protocol, and create mapping for handling responses.
        """
        self.received = ""
        self._mapping = {}
        for name, val in self.__class__.__dict__.iteritems():
            if name.endswith("RESP"):
                name = name.split("_")[0].lower()
                self._mapping[val] = getattr(self, "resp_%s" % (name,))


    def resp_alive2(self, data):
        """
        Manage C{ALIVE2_RESP}.
        """
        if len(data) < 4:
            return data
        result = theParser.parseChar(data[1])
        creation = theParser.parseShort(data[2:4])
        d = self.factory._connectDeferred
        if result == 0:
            # OK
            d.callback(creation)
        else:
            d.errback(ValueError(result))
        return data[4:]


    def resp_port2(self, data):
        """
        Manage C{PORT2_RESP}.
        """
        dataLength = len(data)
        if dataLength < 2:
            return data
        else:
            result = theParser.parseChar(data[1])
            if result != 0:
                d, self.deferred = self.deferred, None
                d.errback(NodeNotFound(result))
                return data[2:]
            if dataLength < 12:
                return data
            portNum = theParser.parseShort(data[2:4])
            nodeType = theParser.parseChar(data[4])
            protocol = theParser.parseChar(data[5])
            distrVSNLo = theParser.parseShort(data[6:8])
            distrVSNHi = theParser.parseShort(data[8:10])
            distrVSNRng = (distrVSNLo, distrVSNHi)
            nLen = theParser.parseShort(data[10:12])
            nodeName = data[12:12 + nLen]
            if (dataLength == 12 + nLen + 1 and
                theParser.parseChar(data[-1]) == 0):
                extra = ""
                data = ""
            else:
                eLen = theParser.parseShort(data[12 + nLen:12 + nLen + 2])
                extra = data[12 + nLen + 2:12 + nLen + 2 + eLen]
                data = data[12 + nLen + 2 + eLen:]
            node = Node(
                portNum, nodeType, protocol, distrVSNRng, nodeName, extra)
            d, self.deferred = self.deferred, None
            d.callback(node)
            return data


    def resp_names(self, data):
        """
        Manage C{NAMES_RESP}.
        """
        epmdPortNumber = theParser.parseInt(data[:4])
        result = []
        for l in data[4:].splitlines():
            infos = l.split()
            # Infos are formatted this way: "name %(name)s at port %(port)s\n"
            result.append((infos[1], int(infos[4])))
        d, self.deferred = self.deferred, None
        d.callback(result)


    def resp_dump(self, data):
        """
        Manage C{DUMP_RESP}.
        """
        epmdPortNumber = theParser.parseInt(data[:4])
        result = {"active": [], "old": []}
        for l in data[4:].split('\n\x00')[:-1]:
            infos = l.split()
            # Active: "active name     <%(name)s> at port %(port)s, fd = %(fd)s\n"
            # Old: "old/unused name, <%(name)s> at port %(port)s, fd = %(fd)s\n"
            if infos[0] == 'active':
                name = infos[2][1:-1]
                extractedInfos = (name, int(infos[5][:-1]), int(infos[8]))
                result['active'].append(extractedInfos)
            elif infos[0] == "old/unused":
                # Remove the trailing comma...
                name = infos[2][1:-2]
                extractedInfos = (name, int(infos[5][:-1]), int(infos[8]))
                result['old'].append(extractedInfos)
        d, self.deferred = self.deferred, None
        d.callback(result)


    def resp_kill(self, data):
        """
        Manage C{KILL_RESP}.
        """
        d, self.deferred = self.deferred, None
        if data == "OK":
            d.callback(data)
        else:
            d.errback(ValueError(data))


    def dataReceived(self, data):
        """
        Dispatch received data to underlying handlers.
        """
        self.received += data
        while True:
            if len(self.received) == 0:
                return
            code = theParser.parseChar(self.received[0])
            if code in self._mapping:
                remainData = self._mapping[code](self.received)
                if len(remainData) == len(self.received):
                    break
                self.received = remainData
            elif self.endDataReceived is not None:
                break
            else:
                log.err(RuntimeError("Unhandled code %s." % (code,)))
                return


    def connectionLost(self, reason):
        """
        Log connection lost.
        """
        log.msg("Connection lost with EPMD: %s" % (reason,))
        if self.endDataReceived is not None:
            self.endDataReceived(self.received)
        if self.deferred is not None:
            d, self.deferred = self.deferred, None
            d.errback(reason)


    def connectionMade(self):
        """
        Send initial requests depending the use of the connection.
        """
        log.msg("Connection made with EPMD")
        if self.initialAction is not None:
           getattr(self, self.initialAction[0])(*self.initialAction[1:])


    def send(self, data):
        """
        Send data to the EPMD.
        """
        msg = thePacker.packShort(len(data)) + data
        self.transport.write(msg)


    def alive2Request(self, portNumber, nodeType, distrVSNRange, nodeName,
                      extra=""):
        """
        Create a C{ALIVE2_REQ} request and send it over the wire.
        """
        data = struct.pack('!BHBBHHH', self.ALIVE2_REQ, portNumber, nodeType,
            0, distrVSNRange[0], distrVSNRange[1], len(nodeName))
        data += nodeName
        data += thePacker.packShort(len(extra))
        data += extra
        self.send(data)


    def portPlease2Request(self, deferred, nodeName):
        """
        Create a C{PORT_PLEASE2_REQ} request and send it over the wire.
        """
        self.deferred = deferred
        data = thePacker.packChar(self.PORT_PLEASE2_REQ) + nodeName
        self.send(data)


    def namesRequest(self, deferred):
        """
        Create a C{NAMES_REQ} request and send it over the wire.
        """
        self.deferred = deferred
        data = thePacker.packChar(self.NAMES_REQ)
        self.send(data)
        self.endDataReceived = self.resp_names


    def dumpRequest(self, deferred):
        """
        Create a C{DUMP_REQ} request and send it over the wire.
        """
        self.deferred = deferred
        data = thePacker.packChar(self.DUMP_REQ)
        self.send(data)
        self.endDataReceived = self.resp_dump


    def killRequest(self, deferred):
        """
        Create a C{KILL_REQ} request and send it over the wire.
        """
        self.deferred = deferred
        data = thePacker.packChar(self.KILL_REQ)
        self.send(data)
        self.endDataReceived = self.resp_kill



class PersistentPortMapperFactory(ClientFactory):
    """
    Client factory for L{PortMapperProtocol}, making a persistent connection
    for an erlang node.
    """
    protocol = PortMapperProtocol
    NODETYPE_HIDDEN = 72
    NODETYPE_NORMAL = 77
    distrVSNRange = (5, 5)
    EPMD_PORT = 4369
    nodeFactoryClass = NodeServerFactory
    _connectDeferred = None
    nodePortNumber = 0

    def __init__(self, nodeName, cookie, host="127.0.0.1"):
        """
        Initialize the factory with the information of the current node.
        """
        self.nodeName = nodeName
        self.cookie = cookie
        self.host = host
        self.nodeType = self.NODETYPE_HIDDEN


    def listenTCP(self, portNumber, factory):
        """
        Wrapper around C{reactor.listenTCP} for tests purpose.
        """
        from twisted.internet import reactor
        return reactor.listenTCP(portNumber, factory)


    def connectTCP(self, host, port, factory):
        """
        Wrapper around C{reactor.connectTCP} for tests purpose.
        """
        from twisted.internet import reactor
        return reactor.connectTCP(host, port, factory)


    def publish(self, **methodsHolder):
        """
        Publish a node with given methods to the EPMD.
        """
        self._connectDeferred = Deferred()
        nodeFactory = self.nodeFactoryClass(methodsHolder, self.nodeName,
                self.cookie, self._connectDeferred)
        nodePort = self.listenTCP(0, nodeFactory)
        self.nodePortNumber = nodePort.getHost().port
        self.connectTCP(self.host, self.EPMD_PORT, self)
        return self._connectDeferred


    def buildProtocol(self, addr):
        """
        Instantiate sub protocol.
        """
        if self.nodePortNumber == 0:
            raise RuntimeError("Local node number not set")
        p = ClientFactory.buildProtocol(self, addr)
        p.initialAction = ("alive2Request",
                           self.nodePortNumber,
                           self.nodeType,
                           self.distrVSNRange,
                           self.nodeName.split("@")[0])
        return p



class OneShotPortMapperFactory(ClientFactory):
    """
    Client factory for L{PortMapperProtocol}.
    """
    protocol = PortMapperProtocol
    EPMD_PORT = 4369
    nodeFactoryClass = NodeClientFactory

    def __init__(self, nodeName, cookie, host="127.0.0.1"):
        """
        Initialize the factory.
        """
        self.nodeName = nodeName
        self.cookie = cookie
        self.host = host
        self.deferreds = []
        self._nodeCache = {}


    def oneShotConnection(self):
        """
        Connections can be made on request to get infos.
        """
        return self.connectTCP(self.host, self.EPMD_PORT, self)


    def onConnectionLost(self, factory):
        """
        Called when a connection is lost with a node.
        """
        if factory.nodeName in self._nodeCache:
            del self._nodeCache[factory.nodeName]


    def buildProtocol(self, addr):
        """
        Instantiate sub protocol.
        """
        p = ClientFactory.buildProtocol(self, addr)
        p.initialAction = self.deferreds.pop(0)
        return p


    def portPlease2Request(self, nodeName):
        """
        Make a port request for given node.
        """
        d = Deferred()
        self.deferreds.append(("portPlease2Request", d, nodeName))
        self.oneShotConnection()
        return d


    def connectTCP(self, host, port, factory, **kwargs):
        """
        Wrapper around C{reactor.connectTCP} for tests purpose.
        """
        from twisted.internet import reactor
        return reactor.connectTCP(host, port, factory, **kwargs)


    def clientConnectionFailed(self, connector, reason):
        d = self.deferreds.pop(0)[1]
        d.errback(reason)


    def connectToNode(self, nodeName, **methodsHolder):
        """
        Get a connection to an erlang node named C{nodeName}.
        """
        if nodeName in self._nodeCache:
            return succeed(self._nodeCache[nodeName])

        def cbPort(node):
            factory = self.nodeFactoryClass(methodsHolder, self.nodeName,
                                            self.cookie, self.onConnectionLost)
            d = factory._connectDeferred
            self.connectTCP(self.host, node.portNumber, factory)
            d.addCallback(cbConnect)
            return d

        def cbConnect(proto):
            self._nodeCache[nodeName] = proto
            return proto

        return self.portPlease2Request(nodeName).addCallback(cbPort)


    def names(self):
        """
        Execute a names request on the EPMD.
        """
        d = Deferred()
        self.deferreds.append(("namesRequest", d))
        self.oneShotConnection()
        return d


    def dump(self):
        """
        Execute a dump request on the EPMD.
        """
        d = Deferred()
        self.deferreds.append(("dumpRequest", d))
        self.oneShotConnection()
        return d


    def kill(self):
        """
        Execute a kill request on the EPMD.
        """
        d = Deferred()
        self.deferreds.append(("killRequest", d))
        self.oneShotConnection()
        return d



class PersistentPortMapperService(Service):
    """
    Persistent port mapper service to be used with twistd.
    """

    def __init__(self, methodsHolder, nodeName, cookie):
        self.methodsHolder = methodsHolder
        self.nodeName = nodeName
        self.cookie = cookie


    def startService(self):
        Service.startService(self)
        epmd = PersistentPortMapperFactory(self.nodeName, self.cookie)
        epmd._connectDeferred = Deferred()
        nodeFactory = epmd.nodeFactoryClass(self.methodsHolder, self.nodeName,
                self.cookie, epmd._connectDeferred)
        server = TCPServer(0, nodeFactory)
        server.startService()
        epmd.nodePortNumber = server._port.getHost().port
        client = TCPClient(epmd.host, epmd.EPMD_PORT, epmd)
        client.startService()
        return epmd._connectDeferred
