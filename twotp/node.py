# -*- test-case-name: twotp.test.test_node -*-
# Copyright (c) 2007-2008 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Basic node protocol and node message handler classes.
"""

import md5, os, time, random

from twisted.internet import protocol, defer
from twisted.python import log

from twotp.term  import Tuple, Atom, Integer, Reference, Pid, List, Port
from twotp.packer import termToBinary, thePacker
from twotp.parser import binaryToTerms, theParser



class InvalidIdentifier(ValueError):
    """
    Exception raised when the packet identifier received wasn't expected.
    """



class InvalidDigest(ValueError):
    """
    Exception raised when the challenge digest received doesn't match.
    """


class BadRPC(ValueError):
    """
    Exception raised when receiving a B{badrpc} answer to a callRemote.
    """


class MessageHandler(object):
    """
    Proxy between erlang protocol and python methods.
    """
    # Early operations
    CTRLMSGOP_LINK = 1
    CTRLMSGOP_SEND = 2
    CTRLMSGOP_EXIT = 3
    CTRLMSGOP_UNLINK = 4
    CTRLMSGOP_NODE_LINK = 5
    CTRLMSGOP_REG_SEND = 6
    CTRLMSGOP_GROUP_LEADER = 7
    CTRLMSGOP_EXIT2 = 8
    # New operations in destrvsn = 1 (OTP R4)
    CTRLMSGOP_SEND_TT = 12
    CTRLMSGOP_EXIT_TT = 13
    CTRLMSGOP_REG_SEND_TT = 16
    CTRLMSGOP_EXIT2_TT = 18
    # New operations in destrvsn = 4 (OTP R6)
    CTRLMSGOP_MONITOR_P = 19
    CTRLMSGOP_DEMONITOR_P = 20
    CTRLMSGOP_MONITOR_P_EXIT = 21

    refIds = None
    pidCount = 0
    portCount = 0
    serial = 0

    methodsHolder = None
    nodeName = ""
    cookie = ""
    creation = 0

    def __init__(self):
        """
        Instantiate the handler and its operation mapping.
        """
        self.refIds = [1, 0, 0]
        self._mapping = {}
        for name, val in MessageHandler.__dict__.iteritems():
            if name.startswith('CTRLMSGOP_'):
                name = name.split('CTRLMSGOP_')[1].lower()
                self._mapping[val] = getattr(self, 'operation_%s' % (name,))
        self._pendingResponses = {}


    def operation_send(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_SEND}.
        """
        destPid = controlMessage[1]
        d = self._pendingResponses[destPid].pop(0)
        d.callback((controlMessage, message))


    def operation_link(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_LINK}.
        """
        raise NotImplementedError()


    def operation_exit(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_EXIT}.
        """
        raise NotImplementedError()


    def operation_unlink(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_UNLINK}.
        """
        raise NotImplementedError()


    def operation_node_link(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_NODE_LINK}.
        """
        raise NotImplementedError()


    def operation_group_leader(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_GROUP_LEADER}.
        """
        raise NotImplementedError()


    def operation_exit2(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_EXIT2}.
        """
        raise NotImplementedError()


    def operation_send_tt(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_SEND_TT}.
        """
        traceToken = controlMessage[2]
        return self.operation_send(proto, controlMessage, message)


    def operation_exit_tt(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_EXIT_TT}.
        """
        traceToken = controlMessage[2]
        return self.operation_exit(proto, controlMessage, message)


    def operation_reg_send_tt(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_REG_SEND_TT}.
        """
        traceToken = controlMessage[3]
        return self.operation_reg_send(proto, controlMessage, message)


    def operation_exit2_tt(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_EXIT2_TT}.
        """
        traceToken = controlMessage[2]
        return self.operation_exit2(proto, controlMessage, message)


    def operation_monitor_p(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_MONITOR_P}.
        """
        raise NotImplementedError()


    def operation_demonitor_p(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_DEMONITOR_P}.
        """
        raise NotImplementedError()


    def operation_monitor_p_exit(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_MONITOR_P_EXIT}.
        """
        raise NotImplementedError()


    def regsend_net_kernel(self, proto, message):
        """
        Handle regsend reply for net_kernel module.
        """
        if message[0].text == "$gen_call":
            if message[2][0].text == "is_auth":
                # Reply to ping
                toPid = message[1][0]
                ref = message[1][1]
                resp = Tuple((ref, Atom("yes")))
            elif message[2][0].text == "spawn":
                toPid = message[1][0]
                ref = message[1][1]
                # Don't handle that for now
                resp = Tuple((ref, Atom("error")))
            else:
                log.msg("Unhandled method %s" % (message[2][0].text,))
                resp = Tuple((ref, Atom("error")))
        else:
            log.msg("Unhandled call %s" % (message[0].text,))
            resp = Tuple((ref, Atom("error")))
        self.send(proto, toPid, resp)


    def regsend_rex(self, proto, message):
        """
        Handle regsend reply for rpc.
        """
        toPid = message[1][0]
        ref = message[1][1]
        module = message[2][1].text
        func = message[2][2].text
        args = message[2][3]
        if module in self.methodsHolder:
            proxy = getattr(self.methodsHolder[module],
                            "remote_%s" % (func,), None)

            if proxy is None:
                log.msg("Unknow method %r of module %r" % (func, module))

                self.send(proto, toPid, Tuple((ref, (Atom('badrpc'),
                         "undefined function %r" % (func,)))))
            else:
                d = defer.maybeDeferred(proxy, *args)
                d.addCallback(self._forwardResponse, proto, toPid, ref)
                d.addErrback(self._forwardError, proto, toPid, ref)
        else:
            log.msg("No holder registered for %r" % (module,))
            self.send(proto, toPid, Tuple((ref, (Atom('badrpc'),
                     "undefined module %r" % (module,)))))


    def operation_reg_send(self, proto, controlMessage, message):
        """
        Handle C{REG_SEND} reply.
        """
        # Unpacked, but unused for now
        fromPid = controlMessage[0]
        cookie = controlMessage[1]
        toName = controlMessage[2]
        subHandler = getattr(self, "regsend_%s" % (toName.text,))
        return subHandler(proto, message)


    def _forwardResponse(self, result, proto, toPid, ref):
        """
        Forward a response to an erlang node from a python method.
        """
        if result is None:
            result = Atom("null")
        else:
            result = Tuple((Atom("ok"), result))
        self.send(proto, toPid, Tuple((ref, result)))


    def _forwardError(self, error, proto, toPid, ref):
        """
        Forward the string representation of the exception to the node.
        """
        log.err(error)
        self.send(proto, toPid,
            Tuple((ref, (Atom('badrpc'), str(error.value)))))


    def send(self, proto, destPid, msg):
        """
        Common routine to reply to a request.
        """
        cookie = Atom('')
        ctrlMsg = Tuple((Integer(self.CTRLMSGOP_SEND), cookie, destPid))
        proto.send("p" + termToBinary(ctrlMsg) + termToBinary(msg))


    def passThroughMessage(self, proto, controlMessage, message=None):
        """
        Forward operation to the methods handling it.
        """
        operation = controlMessage[0]
        self._mapping[operation](proto, controlMessage[1:], message)


    def callRemote(self, proto, module, func, *args):
        """
        Call a RPC method on an erlang node.
        """
        d = defer.Deferred()

        cookie = Atom('')
        srcPid = self.createPid(proto)
        call = Tuple((Atom("call"), Atom(module), Atom(func), List(args),
                      Atom("user")))
        rpc =  Tuple((srcPid, call))

        ctrlMsg = Tuple((Integer(self.CTRLMSGOP_REG_SEND), srcPid,
                         cookie, Atom("rex")))
        self._pendingResponses.setdefault(srcPid, []).append(d)
        proto.send("p" + termToBinary(ctrlMsg) + termToBinary(rpc))
        def cb((ctrlMessage, message)):
            if (isinstance(message[1], (list, tuple)) and
                len(message[1]) > 0 and message[1][0] == Atom("badrpc")):
                raise BadRPC(message[1][1])
            return message[1]
        d.addCallback(cb)
        return d


    def ping(self, proto):
        """
        Ping a remote node.
        """
        d = defer.Deferred()

        cookie = Atom('')
        srcPid = self.createPid(proto)
        ref = self.createRef()
        msg = Tuple((Atom("$gen_call"), Tuple((srcPid, ref)),
                     Tuple((Atom("is_auth"),
                            Atom(self.nodeName)))))
        ctrlMsg = Tuple((Integer(self.CTRLMSGOP_REG_SEND), srcPid,
                         cookie, Atom("net_kernel")))
        self._pendingResponses.setdefault(srcPid, []).append(d)
        proto.send("p" + termToBinary(ctrlMsg) + termToBinary(msg))
        def cb((ctrlMessage, message)):
            if message[1].text == "yes":
                return "pong"
            return "pang"
        d.addCallback(cb)
        return d


    def createRef(self):
        """
        Create an unique erlang reference.
        """
        r =  Reference(Atom(self.nodeName), self.refIds,
                       self.creation)
        self.refIds[0] += 1
        if self.refIds[0] > 0x3ffff:
            self.refIds[0] = 0
            self.refIds[1] += 1
            if isinstance(self.refIds[1], long):
                self.refIds[1] = 0
                self.refIds[2] += 1
        return r


    def createPid(self, proto):
        """
        Create an unique Pid object.
        """
        p = Pid(Atom(self.nodeName), self.pidCount, self.serial,
            self.creation)
        self.pidCount += 1
        if self.pidCount > 0x7fff:
            self.pidCount = 0
            self.serial += 1
            if proto.distrFlags & proto.DISTR_FLAG_EXTENDEDPIDSPORTS:
                if self.serial > 0x1fff:
                    self.serial = 0
            elif self.serial > 0x07:
                self.serial = 0
        return p


    def createPort(self, proto):
        """
        Create an unique Port object.
        """
        o = Port(Atom(self.nodeName), self.portCount, self.creation)
        self.portCount += 1
        if proto.distrFlags & proto.DISTR_FLAG_EXTENDEDPIDSPORTS:
            if self.portCount > 0xfffffff:
                self.portCount = 0
        elif self.portCount > 0x3ffff:
            self.portCount = 0
        return o



class NodeProtocol(protocol.Protocol):
    """
    @ivar state: 'handshake', 'challenge', 'connected'.
    @type state: C{str}
    """

    DISTR_FLAG_PUBLISHED = 1
    DISTR_FLAG_ATOMCACHE = 2
    DISTR_FLAG_EXTENDEDREFERENCES = 4
    DISTR_FLAG_DISTMONITOR = 8
    DISTR_FLAG_FUNTAGS = 16
    DISTR_FLAG_DISTMONITORNAME = 32
    DISTR_FLAG_HIDDENATOMCACHE = 64
    DISTR_FLAG_NEWFUNTAGS = 128
    DISTR_FLAG_EXTENDEDPIDSPORTS = 256

    distrVersion = 5
    distrFlags = DISTR_FLAG_EXTENDEDREFERENCES|DISTR_FLAG_EXTENDEDPIDSPORTS

    def __init__(self):
        """
        Initialize protocol attributes.
        """
        self.received = ""
        self.state = "handshake"
        self._responseTimerID = None
        self._tickTimerID = None
        self._lastResponseTime = 0
        self._lastTickTime = 0


    def callLater(self, interval, f):
        """
        Wrapper around C{reactor.callLater} for test purpose.
        """
        from twisted.internet import reactor
        return reactor.callLater(interval, f)


    def startTimer(self):
        """
        Start timers checking connection activity.
        """
        self._responseTimerID = self.callLater(
                self.factory.netTickTime * 0.25, self._responseTimer)
        self._tickTimerID = self.callLater(
                self.factory.netTickTime * 0.125, self._tickTimer)


    def updateResponseTimer(self):
        """
        Update last time a response was received.
        """
        self._lastResponseTime = self.factory.timeFactory()


    def updateTickTimer(self):
        """
        Update last time a request was sent.
        """
        self._lastTickTime = self.factory.timeFactory()


    def _responseTimer(self):
        """
        Check for last response.
        """
        now = self.factory.timeFactory()
        if now > self._lastResponseTime + self.factory.netTickTime * 1.25:
            log.msg("No response for a long time, disconnecting.")
            self._responseTimerID = None
            self.transport.loseConnection()
        else:
            self._responseTimerID = self.callLater(
                    self.factory.netTickTime * 0.25, self._responseTimer)


    def _tickTimer(self):
        """
        Check for last request, sending a fake request if necessary.
        """
        now = self.factory.timeFactory()
        if now > self._lastTickTime + self.factory.netTickTime * 0.25:
            self._tickTimerID = None
            self.send("")
        self._tickTimerID = self.callLater(
                self.factory.netTickTime * 0.125, self._tickTimer)


    def send_handshake(self, data):
        """
        Send during the handshake state.
        """
        msg = thePacker.packShort(len(data)) + data
        self.transport.write(msg)


    send_challenge = send_handshake


    def send_connected(self, data):
        """
        Send during the connected state.
        """
        msg = thePacker.packInt(len(data)) + data
        self.transport.write(msg)


    def send(self, data):
        """
        Send data: update last sent time and forward current send method.
        """
        self.updateTickTimer()
        return getattr(self, "send_%s" % (self.state,))(data)


    def generateChallenge(self):
        """
        Generate a simple insecure challenge.
        """
        return int(self.factory.randomFactory()) & 0x7fffffff


    def generateDigest(self, challenge, cookie):
        """
        Create a MD5 digest of a challenge and a cookie.
        """
        challengeStr = str(challenge)
        return md5.md5(cookie + challengeStr).digest()


    def dataReceived(self, data):
        """
        Dispatch data between handlers.
        """
        self.received += data
        while self.received:
            remainData = getattr(self, "handle_%s" % (self.state,))(self.received)
            if len(remainData) == len(self.received):
                # Data remains unchanged, so there's nothing more to see here
                break
            self.received = remainData
        self.updateResponseTimer()


    def handle_connected(self, data):
        """
        Handle data in the connected state.
        """
        if len(data) < 4:
            return data
        packetLen = theParser.parseInt(data[0:4])
        if len(data) < packetLen + 4:
            # Incomplete packet.
            return data
        packetData = data[4:packetLen + 4]
        if packetLen == 0:
            # Tick
            pass
        elif packetData[0] == "p":
            terms = list(binaryToTerms(packetData[1:]))
            if len(terms) == 2:
                self.factory.passThroughMessage(self, terms[0], terms[1])
            elif len(terms) == 1:
                self.factory.passThroughMessage(self, terms[0])
            else:
                log.msg("Unhandled terms")
        else:
            log.msg("Unhandled packed type: %r" % (packetData[0],))
        return data[packetLen + 4:]


    def connectionLost(self, reason):
        """
        Manage connection lost with a node.
        """
        log.msg("Connection closed: %s" % (reason,))
        if self._responseTimerID is not None:
            self._responseTimerID.cancel()
            self._responseTimerID = None
        if self._tickTimerID is not None:
            self._tickTimerID.cancel()
            self._tickTimerID = None



class NodeBaseFactory(MessageHandler):
    """
    Mixin factory for client and server node connections.

    @ivar creation: node serial number.
    @type creation: C{int}

    @ivar netTickTime: reference time used for checking node connectivity.
    @type netTickTime: C{int}

    @ivar timeFactory: factory giving current time, to be customized in tests.
    @type timeFactory: C{callable}

    @ivar randomFactory: factory giving a random number, to be customized in
        the tests.
    @type randomFactory: C{callable}
    """
    creation = 0
    netTickTime = 60
    timeFactory = time.time
    randomFactory = random.random

    def __init__(self, methodsHolder, nodeName, cookie):
        """
        Initialize the server factory.

        @param methodsHolder: contain mapping of function into modules for RPC
            calls from an erlang node.
        @type methodsHolder: C{dict}

        @param nodeName: the name of the node.
        @type nodeName: C{str}

        @type cookie: cookie used for authorization between nodes.
        @param cookie: C{str}
        """
        MessageHandler.__init__(self)
        self.methodsHolder = methodsHolder
        self.nodeName = nodeName
        self.cookie = cookie



def buildNodeName(nodeName):
    """
    Check if nodeName is a valid nodeName, or append the current hostname
    to it.
    """
    if "@" in nodeName:
        return nodeName
    else:
        return nodeName + "@" + getHostName()



def getHostName():
    """
    Return the current hostname to be used in the node name.
    """
    import socket
    return socket.getfqdn("").split(".")[0]



def readCookie():
    """
    Read cookie on disk.
    """
    return file(os.path.expanduser('~/.erlang.cookie')).readlines()[0].strip()
