# -*- test-case-name: twotp.test.test_node -*-
# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Basic node protocol and node message handler classes.
"""

import os, time, random

try:
    from hashlib import md5
except ImportError:
    from md5 import md5

from twisted.internet.protocol import Protocol
from twisted.internet.defer import (
    succeed, Deferred, maybeDeferred, TimeoutError)
from twisted.python import log

from twotp.term  import Tuple, Atom, Integer, Reference, Pid, List, Port
from twotp.packer import termToBinary, thePacker
from twotp.parser import ParserWithPidCache



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
    distrFlags = DISTR_FLAG_EXTENDEDREFERENCES|DISTR_FLAG_EXTENDEDPIDSPORTS|DISTR_FLAG_DISTMONITOR

    refIds = None
    pidCount = 0
    portCount = 0
    serial = 0

    nodeName = ""
    cookie = ""
    creation = 0

    def __init__(self, nodeName, cookie):
        """
        Instantiate the handler and its operation mapping.
        """
        self.nodeName = nodeName
        self.cookie = cookie

        self.refIds = [1, 0, 0]
        self._mapping = {}
        for name, val in MessageHandler.__dict__.iteritems():
            if name.startswith('CTRLMSGOP_'):
                name = name.split('CTRLMSGOP_')[1].lower()
                self._mapping[val] = getattr(self, 'operation_%s' % (name,))
        self._parser = ParserWithPidCache()
        self._namedProcesses = {}
        self._registeredProcesses = {}


    def operation_send(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_SEND}.
        """
        destPid = controlMessage[1]
        if destPid in self._registeredProcesses:
            self._registeredProcesses[destPid](proto, controlMessage, message)
        else:
            log.msg("Send to unknown process %r" % (destPid,))


    def operation_exit(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_EXIT}.
        """
        destPid = controlMessage[0]
        srcPid = controlMessage[1]
        srcPid._signalExit(destPid, controlMessage[2])


    def operation_link(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_LINK}.
        """
        srcPid = controlMessage[0]
        destPid = controlMessage[1]
        destPid.link(proto, srcPid)


    def operation_unlink(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_UNLINK}.
        """
        srcPid = controlMessage[0]
        destPid = controlMessage[1]
        destPid.unlink(proto, srcPid)


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
        return self.operation_exit(proto, controlMessage, message)


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
        srcPid = controlMessage[0]
        destPid = controlMessage[1]
        monitorRef = controlMessage[2]
        destPid._remoteMonitor(proto, srcPid, monitorRef)


    def operation_demonitor_p(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_DEMONITOR_P}.
        """
        srcPid = controlMessage[0]
        destPid = controlMessage[1]
        monitorRef = controlMessage[2]
        destPid._remoteDemonitor(proto, srcPid, monitorRef)


    def operation_monitor_p_exit(self, proto, controlMessage, message):
        """
        Manage C{CTRLMSGOP_MONITOR_P_EXIT}.
        """
        srcPid = controlMessage[0]
        destPid = controlMessage[1]
        monitorRef = controlMessage[2]
        destPid._signalMonitorExit(srcPid, monitorRef, controlMessage[3])


    def operation_reg_send(self, proto, controlMessage, message):
        """
        Handle C{REG_SEND} reply.
        """
        # Unpacked, but unused for now
        fromPid = controlMessage[0]
        cookie = controlMessage[1]
        toName = controlMessage[2]
        if toName.text in self._namedProcesses:
           self._namedProcesses[toName.text](proto, controlMessage, message)
        else:
            log.msg("Send to unknown process name %r" % (toName.text,))


    def send(self, proto, destPid, msg):
        """
        Common routine to reply to a request.
        """
        cookie = Atom('')
        ctrlMsg = Tuple((Integer(self.CTRLMSGOP_SEND), cookie, destPid))
        proto.send("p" + termToBinary(ctrlMsg) + termToBinary(msg))


    def namedSend(self, proto, pid, processName, msg):
        """
        Send a message to a named process.
        """
        cookie = Atom('')
        ctrlMsg = Tuple(
            (Integer(self.CTRLMSGOP_REG_SEND), cookie, pid, processName))
        proto.send("p" + termToBinary(ctrlMsg) + termToBinary(msg))


    def passThroughMessage(self, proto, controlMessage, message=None):
        """
        Forward operation to the methods handling it.
        """
        operation = controlMessage[0]
        self._mapping[operation](proto, controlMessage[1:], message)


    def sendLink(self, proto, srcPid, destPid):
        """
        Create a link from local PID C{srcPid} to remote PID C{destPid}.
        """
        ctrlMsg = Tuple((Integer(self.CTRLMSGOP_LINK), srcPid, destPid))
        proto.send("p" + termToBinary(ctrlMsg))


    def sendUnlink(self, proto, srcPid, destPid):
        """
        Remove a previously created link between local PID C{srcPid} to remote
        PID C{destPid}.
        """
        ctrlMsg = Tuple((Integer(self.CTRLMSGOP_UNLINK), srcPid, destPid))
        proto.send("p" + termToBinary(ctrlMsg))


    def sendMonitor(self, proto, srcPid, destPid):
        """
        Monitor remote PID C{destPid}.

        @return: the L{Reference} of the monitoring, which will be passed in
            exit.
        """
        monitorRef = self.createRef()
        ctrlMsg = Tuple(
            (Integer(self.CTRLMSGOP_MONITOR_P), srcPid, destPid, monitorRef))
        proto.send("p" + termToBinary(ctrlMsg))
        return monitorRef


    def sendDemonitor(self, proto, srcPid, destPid, monitorRef):
        """
        Remove monitoring of remote process C{destPid}.

        @return: the L{Reference} of the monitoring, which will be passed in
            exit.
        """
        ctrlMsg = Tuple(
            (Integer(self.CTRLMSGOP_DEMONITOR_P), srcPid, destPid, monitorRef))
        proto.send("p" + termToBinary(ctrlMsg))


    def sendLinkExit(self, proto, srcPid, destPid, reason):
        """
        Send an exit signal for a remote linked process.
        """
        ctrlMsg = Tuple(
            (Integer(self.CTRLMSGOP_EXIT), srcPid, destPid, reason))
        proto.send("p" + termToBinary(ctrlMsg))


    def sendMonitorExit(self, proto, srcPid, destPid, monitorRef, reason):
        """
        Send a monitor exit signal for a remote process.
        """
        ctrlMsg = Tuple(
            (Integer(self.CTRLMSGOP_MONITOR_P_EXIT), srcPid, destPid,
             monitorRef, reason))
        proto.send("p" + termToBinary(ctrlMsg))


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


    def createPid(self):
        """
        Create an unique Pid object.
        """
        p = Pid(Atom(self.nodeName), self.pidCount, self.serial,
            self.creation)
        self.pidCount += 1
        if self.pidCount > 0x7fff:
            self.pidCount = 0
            self.serial += 1
            if self.distrFlags & self.DISTR_FLAG_EXTENDEDPIDSPORTS:
                if self.serial > 0x1fff:
                    self.serial = 0
            elif self.serial > 0x07:
                self.serial = 0
        self._parser._pids[p] = p
        return p


    def createPort(self):
        """
        Create an unique Port object.
        """
        o = Port(Atom(self.nodeName), self.portCount, self.creation)
        self.portCount += 1
        if self.distrFlags & self.DISTR_FLAG_EXTENDEDPIDSPORTS:
            if self.portCount > 0xfffffff:
                self.portCount = 0
        elif self.portCount > 0x3ffff:
            self.portCount = 0
        return o



class NodeProtocol(Protocol):
    """
    @ivar state: 'handshake', 'challenge', 'connected'.
    @type state: C{str}
    """

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
        return md5(cookie + challengeStr).digest()


    def dataReceived(self, data):
        """
        Dispatch data between handlers.
        """
        self.received += data
        while self.received:
            remainData = getattr(
                self, "handle_%s" % (self.state,))(self.received)
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
        packetLen = self.factory.handler._parser.parseInt(data[0:4])
        if len(data) < packetLen + 4:
            # Incomplete packet.
            return data
        packetData = data[4:packetLen + 4]
        if packetLen == 0:
            # Tick
            pass
        elif packetData[0] == "p":
            terms = list(
                self.factory.handler._parser.binaryToTerms(packetData[1:]))
            if len(terms) == 2:
                self.factory.handler.passThroughMessage(
                    self, terms[0], terms[1])
            elif len(terms) == 1:
                self.factory.handler.passThroughMessage(self, terms[0])
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



class NodeBaseFactory(object):
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

    def __init__(self, nodeName, cookie):
        """
        Initialize the server factory.

        @param nodeName: the name of the node.
        @type nodeName: C{str}

        @type cookie: cookie used for authorization between nodes.
        @param cookie: C{str}
        """
        self.handler = MessageHandler(nodeName, cookie)



class ProcessExited(Exception):
    """
    Exception raised when trying to use an exited process.
    """



class ProcessBase(object):
    """
    Represent the an Erlang-like process in your Twisted application, able to
    communicate with Erlang nodes.
    """
    persistentEpmd = None
    serverFactory = None
    pid = None
    _receiveDeferred = None
    _cancelReceiveID = None

    def __init__(self, nodeName, cookie, handler=None):
        self.nodeName = nodeName
        self.cookie = cookie
        self.oneShotEpmds = {}
        self._pendingReceivedData = []
        if handler is None:
            handler = MessageHandler(nodeName, cookie)
        self.handler = handler
        self.pid = self.handler.createPid()
        self.handler._registeredProcesses[self.pid] = self._receivedData


    def callLater(self, interval, f):
        """
        Wrapper around C{reactor.callLater} for test purpose.
        """
        from twisted.internet import reactor
        return reactor.callLater(interval, f)


    def oneShotPortMapperClass(self):
        """
        Property serving L{OneShotPortMapperFactory}, to be customized in
        tests.
        """
        from twotp.epmd import OneShotPortMapperFactory
        return OneShotPortMapperFactory
    oneShotPortMapperClass = property(oneShotPortMapperClass)


    def persistentPortMapperClass(self):
        """
        Property serving L{PersistentPortMapperFactory}, to be customized in
        tests.
        """
        from twotp.epmd import PersistentPortMapperFactory
        return PersistentPortMapperFactory
    persistentPortMapperClass = property(persistentPortMapperClass)


    def listen(self):
        """
        Start a listening process able to receive calls from other Erlang
        nodes.
        """
        if self.persistentEpmd is not None:
            raise RuntimeError("Already listening")
        self.persistentEpmd = self.persistentPortMapperClass(
            self.nodeName, self.cookie)
        def gotFactory(factory):
            self.serverFactory = factory
            factory.handler = self.handler
        return self.persistentEpmd.publish().addCallback(gotFactory)


    def _getNodeConnection(self, nodeName):
        """
        Retrieve a connection to node C{nodeName}.
        """
        nodeName = buildNodeName(nodeName)
        if (self.serverFactory is not None and
            nodeName in self.serverFactory._nodeCache):
            return succeed(self.serverFactory._nodeCache[nodeName])
        def sync(instance):
            instance.factory.handler = self.handler
            return instance
        host = nodeName.split("@", 1)[1]
        if not host in self.oneShotEpmds:
            self.oneShotEpmds[host] = self.oneShotPortMapperClass(
                self.nodeName, self.cookie, host)
        oneShotEpmd = self.oneShotEpmds[host]
        return oneShotEpmd.connectToNode(nodeName).addCallback(sync)


    def register(self, name):
        """
        Register this process with name C{name}.
        """
        self.handler._namedProcesses[name] = self._receivedData


    def ping(self, nodeName):
        """
        Ping node C{nodeName}.
        """
        def doPing(instance):
            d = Deferred()
            process = NetKernelResponseProcess(
                self.nodeName, self.cookie, self.handler, d)
            pid = process.pid

            ref = self.handler.createRef()
            msg = Tuple((Atom("$gen_call"), Tuple((pid, ref)),
                         Tuple((Atom("is_auth"), Atom(self.nodeName)))))
            self.handler.namedSend(instance, pid, Atom("net_kernel"), msg)
            return d
        return self._getNodeConnection(nodeName).addCallback(doPing)


    def callRemote(self, nodeName, module, func, *args):
        """
        RPC call against node C{nodeName}.
        """
        def doCallRemote(instance):
            d = Deferred()
            process = RexResponseProcess(
                self.nodeName, self.cookie, self.handler, d)
            pid = process.pid

            call = Tuple((Atom("call"), Atom(module), Atom(func), List(args),
                          Atom("user")))
            rpc =  Tuple((pid, call))

            self.handler.namedSend(instance, pid, Atom("rex"), rpc)
            return d
        return self._getNodeConnection(nodeName).addCallback(doCallRemote)


    def whereis(self, nodeName, process):
        """
        Return the pid of a process on a node.
        """
        def check(result):
            if isinstance(result, Atom) and result.text == "undefined":
                raise ValueError("Process not found")
            elif isinstance(result, Pid):
                return result
            else:
                raise ValueError("Unexpected result", result)
        return self.callRemote(nodeName, "erlang", "whereis", Atom(process)
            ).addCallback(check)


    def link(self, pid):
        """
        Create a link with process C{pid}.
        """
        def doLink(instance):
            self.pid.link(instance, pid)
            self.handler.sendLink(instance, self.pid, pid)
        return self._getNodeConnection(pid.nodeName.text).addCallback(doLink)


    def unlink(self, pid):
        """
        Remove a link with process C{pid}.
        """
        def doUnlink(instance):
            self.pid.unlink(instance, pid)
            self.handler.sendUnlink(instance, self.pid, pid)
        return self._getNodeConnection(pid.nodeName.text
            ).addCallback(doUnlink)


    def monitor(self, pid):
        """
        Monitor process C{pid}.

        @return: a L{Reference} of the monitoring link.
        """
        def doMonitor(instance):
            return self.handler.sendMonitor(instance, self.pid, pid)
        return self._getNodeConnection(pid.nodeName.text
            ).addCallback(doMonitor)


    def demonitor(self, pid, ref):
        """
        Demonitor process C{pid}.
        """
        def doDemonitor(instance):
            return self.handler.sendDemonitor(instance, self, pid, ref)
        return self._getNodeConnection(pid.nodeName.text
            ).addCallback(doDemonitor)


    def send(self, pid, msg):
        """
        Directy send data to process C{pid}.
        """
        def doSend(instance):
            return self.handler.send(instance, pid, msg)
        return self._getNodeConnection(pid.nodeName.text).addCallback(doSend)


    def namedSend(self, nodeName, processName, msg):
        """
        Send data to process named C{processName} on node C{nodeName}.
        """
        def doSend(instance):
            return self.handler.namedSend(
                instance, self.pid, Atom(processName), msg)
        return self._getNodeConnection(nodeName).addCallback(doSend)


    def _receivedData(self, proto, ctrlMessage, message):
        """
        Callback called when a message has been send to this pid.
        """
        if self._receiveDeferred is not None:
            if self._cancelReceiveID is not None:
                self._cancelReceiveID.cancel()
                self._cancelReceiveID = None
            d, self._receiveDeferred = self._receiveDeferred, None
            d.callback(message)
        else:
            self._pendingReceivedData.append(message)


    def _cancelReceive(self):
        """
        Cancel a receive with a specified timeout.

        @see: C{receive}
        """
        self._cancelReceiveID = None
        d, self._receiveDeferred = self._receiveDeferred, None
        d.errback(TimeoutError())


    def receive(self, timeout=None):
        """
        Wait for received data on this process, possibly specifying a timeout.

        @param timeout: timeout in seconds to wait for data.
        @type timeout: C{int} or C{NoneType}

        @return: a L{Deferred} that will fire received data.
        """
        if self._pendingReceivedData:
            return succeed(self._pendingReceivedData.pop(0))
        elif self._receiveDeferred is None:
            self._receiveDeferred = Deferred()
            if timeout is not None:
                self._cancelReceiveID = self.callLater(
                    timeout, self._cancelReceive)
            return self._receiveDeferred
        else:
            raise RuntimeError("Pending receive")


    def exit(self, reason):
        """
        Exit this process.
        """
        for proto, pid in self.pid._links:
            self.handler.sendLinkExit(proto, self, pid, reason)
        for proto, pid, ref in self.pid._remoteMonitors:
            self.handler.sendMonitorExit(proto, self, pid, ref, reason)
        self.pid.exit(reason)
        if self._receiveDeferred is not None:
            d, self._receiveDeferred = self._receiveDeferred, None
            d.errback(ProcessExited(reason))


    def addExitHandler(self, pid, handler):
        """
        Register a callback to be called when C{pid} exits.
        """
        self.pid._handlers.setdefault(pid, []).append(handler)


    def addMonitorHandler(self, ref, handler):
        """
        Register a callback to be called when monitoring L{Reference} C{ref}
        fires.
        """
        self.pid._monitorHandlers.setdefault(ref, []).append(handler)



class Process(ProcessBase):
    """
    The master process, managing C{rex} and C{net_kernel} process.
    """

    def __init__(self, nodeName, cookie):
        ProcessBase.__init__(self, nodeName, cookie)
        self._methodsHolder = {}
        rex = RexProcess(nodeName, cookie, self.handler, self._methodsHolder)
        rex.register("rex")
        netKernel = NetKernelProcess(nodeName, cookie, self.handler)
        netKernel.register("net_kernel")


    def registerModule(self, name, instance):
        """
        Register a method holder for module named C{name}.
        """
        self._methodsHolder[name] = instance



class RexProcess(ProcessBase):
    """
    The C{rex} process: specific process able to receive RPC calls.
    """

    def __init__(self, nodeName, cookie, handler, methodsHolder):
        ProcessBase.__init__(self, nodeName, cookie, handler)
        self._methodsHolder = methodsHolder


    def _receivedData(self, proto, ctrlMessage, message):
        """
        Parse messages and forward data to the appropriate method, if any.
        """
        toPid = message[1][0]
        ref = message[1][1]
        module = message[2][1].text
        func = message[2][2].text
        args = message[2][3]
        if module in self._methodsHolder:
            proxy = getattr(self._methodsHolder[module],
                            "remote_%s" % (func,), None)

            if proxy is None:
                log.msg("Unknow method %r of module %r" % (func, module))

                self.handler.send(proto, toPid, Tuple((ref, (Atom('badrpc'),
                                  "undefined function %r" % (func,)))))
            else:
                log.msg("Remote call to method %r" % (func,))
                d = maybeDeferred(proxy, *args)
                d.addCallback(self._forwardResponse, proto, toPid, ref)
                d.addErrback(self._forwardError, proto, toPid, ref)
        else:
            log.msg("No holder registered for %r" % (module,))
            self.handler.send(proto, toPid, Tuple((ref, (Atom('badrpc'),
                              "undefined module %r" % (module,)))))


    def _forwardResponse(self, result, proto, toPid, ref):
        """
        Forward a response to an erlang node from a python method.
        """
        if result is None:
            result = Atom("null")
        else:
            result = Tuple((Atom("ok"), result))
        self.handler.send(proto, toPid, Tuple((ref, result)))


    def _forwardError(self, error, proto, toPid, ref):
        """
        Forward the string representation of the exception to the node.
        """
        log.handler.err(error)
        self.send(proto, toPid,
            Tuple((ref, (Atom('badrpc'), str(error.value)))))



class RexResponseProcess(ProcessBase):
    """
    A volatile process used to manage one response to a callRemote.
    """

    def __init__(self, nodeName, cookie, handler, deferred):
        ProcessBase.__init__(self, nodeName, cookie, handler)
        self.deferred = deferred


    def _receivedData(self, proto, ctrlMessage, message):
        """
        Parse the message and fire the deferred with appropriate content.
        """
        d = self.deferred
        if (isinstance(message[1], (list, tuple)) and
            len(message[1]) > 0 and message[1][0] == Atom("badrpc")):
            d.errback(BadRPC(message[1][1]))
        else:
            d.callback(message[1])



class NetKernelProcess(ProcessBase):
    """
    A process managing net_kernel calls: it only implements ping responses for
    now.
    """

    def _receivedData(self, proto, ctrlMessage, message):
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
        self.handler.send(proto, toPid, resp)



class NetKernelResponseProcess(ProcessBase):
    """
    A process managing net_kernel responses: it only implements ping for now.
    """

    def __init__(self, nodeName, cookie, handler, deferred):
        ProcessBase.__init__(self, nodeName, cookie, handler)
        self.deferred = deferred


    def _receivedData(self, proto, ctrlMessage, message):
        """
        Handle ping reply.
        """
        d = self.deferred
        if message[1].text == "yes":
            d.callback("pong")
        else:
            d.callback("pang")



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
