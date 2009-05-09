# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Test basic node functionalities.
"""

from twisted.internet.task import Clock
from twisted.test.proto_helpers import StringTransportWithDisconnection

from twotp.node import NodeProtocol, buildNodeName, getHostName, MessageHandler
from twotp.node import Process
from twotp.server import NodeServerFactory
from twotp.term import Pid, Atom, Reference
from twotp.test.util import TestCase

from twotp.test.test_epmd import TestablePPMF, TestableOSPMF
from twotp.test.test_client import DummyClientFactory



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



class DummyFactory(object):
    """
    A dummy factory for tests.
    """

    def __init__(self):
        """
        Initialize with testable values.
        """
        self.times = range(1, 10)
        self.netTickTime = 1
        self.creation = 2
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



class TestableNodeProtocol(NodeProtocol):
    """
    A C{NodeProtocol} with a time-related predictable behavior.
    """

    def __init__(self):
        """
        Create a clock that can be used later in the tests.
        """
        NodeProtocol.__init__(self)
        self.clock = Clock()
        self.callLater = self.clock.callLater



class NodeProtocolTestCase(TestCase):
    """
    Tests for L{NodeProtocol}.
    """

    def setUp(self):
        """
        Create an instance of protocol.
        """
        self.factory = DummyFactory()
        self.proto = TestableNodeProtocol()
        self.transport = CloseNotifiedTransport()
        self.proto.factory = self.factory
        self.proto.makeConnection(self.transport)
        self.transport.protocol = self.proto


    def test_noResponseTimer(self):
        """
        If no message are received for a period of time, connection is dropped.
        """
        self.proto.startTimer()
        self.assertFalse(self.transport.closed)
        self.proto.clock.advance(1)
        self.assertTrue(self.transport.closed)


    def test_noResponseTimerAfterOneResponse(self):
        """
        The response timer reschedules itself.
        """
        self.factory.times = [1, 1, 1, 1, 1, 4]
        self.proto.state = "connected"
        self.proto.startTimer()
        originalCallID = self.proto._responseTimerID
        self.assertFalse(self.transport.closed)
        self.proto.dataReceived("\x00\x00\x00\x00")
        self.proto.clock.advance(1)
        self.assertFalse(self.transport.closed)
        self.assertNotIdentical(self.proto._responseTimerID, originalCallID)
        self.proto.clock.advance(1)
        self.assertTrue(self.transport.closed)


    def test_noTickTimer(self):
        """
        If no message are sent for a period of time, an empty mesage is sent.
        """
        called = []
        def send(data):
            called.append(data)
        self.proto.send = send
        self.proto.startTimer()
        self.assertEquals(called, [])
        self.proto.clock.advance(1)
        self.assertEquals(called, [""])


    def test_sendHandshake(self):
        """
        Test a send during the C{handshake} state.
        """
        self.proto.state = "handshake"
        self.proto.send("foo")
        self.assertEquals(self.transport.value(), "\x00\x03foo")


    def test_sendChallenge(self):
        """
        Test a send during the C{challenge} state.
        """
        self.proto.state = "challenge"
        self.proto.send("bar")
        self.assertEquals(self.transport.value(), "\x00\x03bar")


    def test_sendConnected(self):
        """
        Test a send during the C{connected} state.
        """
        self.proto.state = "connected"
        self.proto.send("egg")
        self.assertEquals(self.transport.value(), "\x00\x00\x00\x03egg")


    def test_generateChallenge(self):
        """
        Test output value of generateChallenge: it truncates data on 28 bits.
        """
        self.assertEquals(self.proto.generateChallenge(), 2)
        data = [0x7fffffff + 2]
        self.factory.randomFactory = data.pop
        self.assertEquals(self.proto.generateChallenge(), 1)


    def test_generateDigest(self):
        """
        Test output value of generateDigest.
        """
        self.assertEquals(self.proto.generateDigest(123, "test_cookie"),
                          "\x15f\x1c\xe3\x92\x8c\xf9\xfd\xf16R?X\x86\x95L")


    def test_messageReceived(self):
        """
        Test the reception of a message.
        """
        self.proto.state = "connected"
        calls = []
        def cb(proto, result):
            calls.append((proto, result))
        self.factory.handler.passThroughMessage = cb
        self.proto.dataReceived("\x00\x00\x00\x06p\x83h\x01a\x01")
        self.assertEquals(calls, [(self.proto, (1,))])



class UtilitiesTestCase(TestCase):
    """
    Tests for utilities functions.
    """

    def test_buildNodeName(self):
        """
        Tests for C{buildNodeName}.
        """
        nodeName = buildNodeName("foo@bar")
        self.assertEquals(nodeName, "foo@bar")
        nodeName = buildNodeName("foo")
        self.assertIn("@", nodeName)


    def test_getHostName(self):
        """
        Tests for C{getHostName}: it returns a non empty string.
        """
        hostName = getHostName()
        self.assertIsInstance(hostName, str)
        self.assertNotEquals(hostName, "")



class MessageHandlerTestCase(TestCase):
    """
    Test for the message handler class.
    """

    def setUp(self):
        """
        Create a C{MessageHandler} for the tests.
        """
        self.handler = MessageHandler("spam@egg", "test_cookie")


    def test_link(self):
        """
        Test handling of a LINK token.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        ctrlMessage = (self.handler.CTRLMSGOP_LINK, srcPid, destPid)
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEquals(destPid._links, set([(None, srcPid)]))


    def test_unlinkNotExisting(self):
        """
        Test handling of an UNLINK token while the link doesn't exit locally.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        ctrlMessage = (self.handler.CTRLMSGOP_UNLINK, srcPid, destPid)
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEquals(destPid._links, set([]))


    def test_unlink(self):
        """
        Test handling of an UNLINK token.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        destPid.link(None, srcPid)
        # Sanity check
        self.assertNotEquals(destPid._links, set([]))

        ctrlMessage = (self.handler.CTRLMSGOP_UNLINK, srcPid, destPid)
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEquals(destPid._links, set([]))


    def test_nodeLink(self):
        """
        Test handling of a NODE_LINK token.
        """
        self.assertRaises(NotImplementedError,
            self.handler.passThroughMessage, None,
            (self.handler.CTRLMSGOP_NODE_LINK,), None)


    def test_groupLeader(self):
        """
        Test handling of a GROUP_LEADER token.
        """
        self.assertRaises(NotImplementedError,
            self.handler.passThroughMessage, None,
            (self.handler.CTRLMSGOP_GROUP_LEADER,), None)


    def test_exit(self):
        """
        Test handling EXIT token.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        destPid.link(None, srcPid)
        called = []
        destPid._handlers[srcPid] = [lambda *args: called.append(args)]

        ctrlMessage = (self.handler.CTRLMSGOP_EXIT, srcPid, destPid, "reason")
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEquals(called, [("reason",)])
        self.assertEquals(destPid._links, set([]))


    def test_exit2(self):
        """
        Test handling of an EXIT2 token.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        destPid.link(None, srcPid)
        called = []
        destPid._handlers[srcPid] = [lambda *args: called.append(args)]

        ctrlMessage = (self.handler.CTRLMSGOP_EXIT2, srcPid, destPid, "reason")
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEquals(called, [("reason",)])
        self.assertEquals(destPid._links, set([]))


    def test_exitTT(self):
        """
        Test handling of an EXIT_TT token.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        destPid.link(None, srcPid)
        called = []
        destPid._handlers[srcPid] = [lambda *args: called.append(args)]

        ctrlMessage = (self.handler.CTRLMSGOP_EXIT_TT, srcPid, destPid,
                       "reason", "TOKEN")
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEquals(called, [("reason",)])
        self.assertEquals(destPid._links, set([]))


    def test_exit2TT(self):
        """
        Test handling of an EXIT2_TT token.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        destPid.link(None, srcPid)
        called = []
        destPid._handlers[srcPid] = [lambda *args: called.append(args)]

        ctrlMessage = (self.handler.CTRLMSGOP_EXIT2_TT, srcPid, destPid,
                       "reason", "TOKEN")
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEquals(called, [("reason",)])
        self.assertEquals(destPid._links, set([]))


    def test_monitorP(self):
        """
        Test handling of a MONITOR_P token.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)
        ctrlMessage = (self.handler.CTRLMSGOP_MONITOR_P, srcPid, destPid, ref)
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEquals(destPid._remoteMonitors, set([(None, srcPid, ref)]))


    def test_demonitorPNotExisting(self):
        """
        Test handling of a DEMONITOR_P token while the link doesn't exist
        locally.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)
        ctrlMessage = (self.handler.CTRLMSGOP_DEMONITOR_P, srcPid, destPid, ref)
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEquals(destPid._remoteMonitors, set([]))


    def test_demonitorP(self):
        """
        Test handling of a DEMONITOR_P token.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)
        destPid._remoteMonitor(None, srcPid, ref)
        # Sanity check
        self.assertNotEquals(destPid._remoteMonitors, set([]))

        ctrlMessage = (self.handler.CTRLMSGOP_DEMONITOR_P, srcPid, destPid, ref)
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEquals(destPid._remoteMonitors, set([]))


    def test_monitorPExit(self):
        """
        Test handling of a MONITOR_P_EXIT token.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)

        ctrlMessage = (self.handler.CTRLMSGOP_MONITOR_P_EXIT, srcPid, destPid,
                       ref, "reason")
        self.handler.passThroughMessage(None, ctrlMessage, None)


    def test_monitorPExitWithHandler(self):
        """
        Test handling of a MONITOR_P_EXIT token with a registered handler to
        it.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)

        called = []
        destPid._monitorHandlers[ref] = [lambda *args: called.append(args)]
        ctrlMessage = (self.handler.CTRLMSGOP_MONITOR_P_EXIT, srcPid, destPid,
                       ref, "reason")
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEquals(called, [("reason",)])


    def test_operationRegSendUnhandled(self):
        """
        Test handling of a REG_SEND token, for an unknown method.
        """
        pid = Pid(Atom("foo@bar"), 0, 0, 0)
        ctrlMessage = (self.handler.CTRLMSGOP_REG_SEND, pid, "cookie",
                       Atom("foo"))
        self.handler.passThroughMessage(None, ctrlMessage, None)


    def test_createPid(self):
        """
        Test L{MessageHandler.createPid}.
        """
        proto = TestableNodeProtocol()
        pid = self.handler.createPid()
        self.assertEquals(pid.nodeId, 0)
        self.assertEquals(pid.serial, 0)
        pid = self.handler.createPid()
        self.assertEquals(pid.nodeId, 1)
        self.assertEquals(pid.serial, 0)


    def test_createPidSerialIncrement(self):
        """
        Check that L{MessageHandler.createPid} increments the serial number
        used for pid when the nodeId reaches the 0x7fff value.
        """
        proto = TestableNodeProtocol()
        self.handler.pidCount = 0x7fff
        pid = self.handler.createPid()
        self.assertEquals(pid.nodeId, 32767)
        self.assertEquals(pid.serial, 0)

        pid = self.handler.createPid()
        self.assertEquals(pid.nodeId, 0)
        self.assertEquals(pid.serial, 1)


    def test_createPidSerialReset(self):
        """
        Check that L{MessageHandler.createPid} resets the serial number to 0
        when the nodeId reaches the 0x7fff value and the serial value reaches
        the 0x1fff value.
        """
        proto = TestableNodeProtocol()
        self.handler.pidCount = 0x7fff
        self.handler.serial = 0x1fff
        pid = self.handler.createPid()
        self.assertEquals(pid.nodeId, 32767)
        self.assertEquals(pid.serial, 8191)

        pid = self.handler.createPid()
        self.assertEquals(pid.nodeId, 0)
        self.assertEquals(pid.serial, 0)


    def test_createPidSerialResetNotExtented(self):
        """
        Check that L{MessageHandler.createPid} resets the serial number to 0
        when the nodeId reaches the 0x7fff value and the serial value reaches
        the 0x7 value and that the protocol distribution flags doesn't specify
        L{DISTR_FLAG_EXTENDEDPIDSPORTS}.
        """
        self.handler.distrFlags -= self.handler.DISTR_FLAG_EXTENDEDPIDSPORTS
        proto = TestableNodeProtocol()
        proto.distrFlags = 0
        self.handler.pidCount = 0x7fff
        self.handler.serial = 0x07
        pid = self.handler.createPid()
        self.assertEquals(pid.nodeId, 32767)
        self.assertEquals(pid.serial, 7)

        pid = self.handler.createPid()
        self.assertEquals(pid.nodeId, 0)
        self.assertEquals(pid.serial, 0)


    def test_createPort(self):
        """
        Test L{MessageHandler.createPort}.
        """
        proto = TestableNodeProtocol()
        port = self.handler.createPort()
        self.assertEquals(port.portId, 0)

        port = self.handler.createPort()
        self.assertEquals(port.portId, 1)


    def test_createPortReset(self):
        """
        L{MessageHandler.createPort} resets the port value to 0 when it reaches
        the 0xfffffff value.
        """
        proto = TestableNodeProtocol()
        self.handler.portCount = 0xfffffff
        port = self.handler.createPort()
        self.assertEquals(port.portId, 268435455)

        port = self.handler.createPort()
        self.assertEquals(port.portId, 0)


    def test_createPortNotExtended(self):
        """
        Check that L{MessageHandler.createPort} reset the port value to 0 when
        it reaches the 0x3ffff value and that the protocol distribution flags
        doesn't specify L{DISTR_FLAG_EXTENDEDPIDSPORTS}.
        """
        self.handler.distrFlags -= self.handler.DISTR_FLAG_EXTENDEDPIDSPORTS
        proto = TestableNodeProtocol()
        proto.distrFlags = 0
        self.handler.portCount = 0x3ffff
        port = self.handler.createPort()
        self.assertEquals(port.portId, 262143)

        port = self.handler.createPort()
        self.assertEquals(port.portId, 0)



class TestableProcessOSMPF(TestableOSPMF):
    """
    An even more testable L{OneShotPortMapperFactory}.
    """

    def __init__(self, *args, **kwargs):
        TestableOSPMF.__init__(self, *args, **kwargs)
        self.factories = []
        self.factoriesArgs = []


    def nodeFactoryClass(self, *args):
        """
        Keep track of the created factory and its arguments.
        """
        clientFactory = DummyClientFactory()
        self.factoriesArgs.append(args)
        self.factories.append(clientFactory)
        return clientFactory



class TestableProcess(Process):
    """
    A testable version of L{Process}.
    """

    def oneShotPortMapperClass(self):
        """
        Return L{TestableProcessOSMPF} instead of L{OneShotPortMapperFactory}
        for making tests easier.
        """
        return TestableProcessOSMPF
    oneShotPortMapperClass = property(oneShotPortMapperClass)


    def persistentPortMapperClass(self):
        """
        Return L{TestablePPMF} instead of L{PersistentPortMapperFactory} for
        making tests easier.
        """
        return TestablePPMF
    persistentPortMapperClass = property(persistentPortMapperClass)



class ProcessTestCase(TestCase):
    """
    Tests for L{Process}.
    """

    def setUp(self):
        self.clock = Clock()
        self.process = TestableProcess("foo@bar", "test_cookie")
        self.process.callLater = self.clock.callLater


    def test_listen(self):
        """
        L{Process.listen} creates an instance of
        L{PersistentPortMapperFactory}, and asks it to publish to the EPMD.
        Once done, it set the calls handler of the server factory to the
        process calls handler.
        """
        holder = object()
        d = self.process.listen()
        self.process.persistentEpmd._connectDeferred.callback(2)
        def check(ignored):
            factory = self.process.serverFactory
            self.assertIsInstance(factory, NodeServerFactory)
            self.assertIdentical(factory.handler, self.process.handler)
        return d.addCallback(check)


    def test_getNodeConnection(self):
        """
        L{Process._getNodeConnection}, if no connection is established, as a
        connection to a L{OneShotPortMapperFactory}. Once it gets a connection
        it sets the calls handler to the client factory to the process handler.
        """
        d = self.process._getNodeConnection("egg@spam")

        epmd = self.process.oneShotEpmds["spam"]
        transport = StringTransportWithDisconnection()
        proto = epmd.buildProtocol(("127.0.01", 4369))
        proto.makeConnection(transport)
        transport.protocol = proto
        self.assertEquals(transport.value(), "\x00\x04zegg")
        self.assertEquals(epmd.connect, [("spam", 4369, epmd)])
        proto.dataReceived(
            "w\x00\x00\x09M\x01\x00\x05\x00\x05\x00\x03bar\x00")

        [factory] = epmd.factories
        self.assertEquals(
            epmd.factoriesArgs,
            [("foo@bar", "test_cookie", epmd.onConnectionLost)])

        clientProto = TestableNodeProtocol()
        clientProto.factory = factory
        factory._connectDeferred.callback(clientProto)
        def check(proto):
            self.assertIdentical(proto, clientProto)
            self.assertIdentical(factory.handler, self.process.handler)
        return d.addCallback(check)


    def test_getNodeConnectionFromServerCache(self):
        """
        L{Process._getNodeConnection} uses the server factory cache to retrieve
        connections, if they exist in here.
        """
        class FakeServerFactory(object):
            _nodeCache = None

        self.process.serverFactory = FakeServerFactory()
        instance = object()
        self.process.serverFactory._nodeCache = {"egg@spam": instance}
        d = self.process._getNodeConnection("egg@spam")
        d.addCallback(self.assertIdentical, instance)
        return d


    def test_ping(self):
        """
        L{Process.ping} allows to make a ping request against another node.
        """
        class FakeServerFactory(object):
            _nodeCache = None

        self.process.serverFactory = FakeServerFactory()
        factory = DummyFactory()
        proto = TestableNodeProtocol()
        proto.state = "connected"
        transport = CloseNotifiedTransport()
        proto.factory = factory
        proto.makeConnection(transport)
        transport.protocol = proto
        self.process.serverFactory._nodeCache = {"egg@spam": proto}

        pids = set(self.process.handler._parser._pids)
        d = self.process.ping("egg@spam")
        self.assertEquals(
            transport.value(),
            "\x00\x00\x00\x7fp\x83h\x04a\x06d\x00\x00gd\x00\x07foo@bar\x00"
            "\x00\x00\x03\x00\x00\x00\x00\x00d\x00\nnet_kernel\x83h\x03d\x00\t"
            "$gen_callh\x02gd\x00\x07foo@bar\x00\x00\x00\x03\x00\x00\x00\x00"
            "\x00r\x00\x03d\x00\x07foo@bar\x00\x00\x00\x00\x01\x00\x00\x00"
            "\x00\x00\x00\x00\x00h\x02d\x00\x07is_authd\x00\x07foo@bar")
        pid = list(set(self.process.handler._parser._pids) - pids)[0]
        ref = Reference(Atom("foo@bar"), 0, 0)
        yes = Atom("yes")

        self.process.handler.operation_send(proto, (Atom(""), pid), (ref, yes))
        return d.addCallback(self.assertEquals, "pong")


    def test_callRemote(self):
        """
        Test L{Process.callRemote}.
        """
        class FakeServerFactory(object):
            _nodeCache = None

        self.process.serverFactory = FakeServerFactory()
        factory = DummyFactory()
        proto = TestableNodeProtocol()
        proto.state = "connected"
        transport = CloseNotifiedTransport()
        proto.factory = factory
        proto.makeConnection(transport)
        transport.protocol = proto
        self.process.serverFactory._nodeCache = {"egg@spam": proto}

        pids = set(self.process.handler._parser._pids)
        d = self.process.callRemote("egg@spam", "module1", "func1", "arg", 1)
        self.assertEquals(
            transport.value(),
            "\x00\x00\x00jp\x83h\x04a\x06d\x00\x00gd\x00\x07foo@bar\x00\x00"
            "\x00\x03\x00\x00\x00\x00\x00d\x00\x03rex\x83h\x02gd\x00\x07"
            "foo@bar\x00\x00\x00\x03\x00\x00\x00\x00\x00h\x05d\x00\x04calld"
            "\x00\x07module1d\x00\x05func1l\x00\x00\x00\x02k\x00\x03arga\x01"
            "jd\x00\x04user")
        pid = list(set(self.process.handler._parser._pids) - pids)[0]

        self.process.handler.operation_send(
            proto, (Atom(""), pid), (Atom("rex"), [2, "arg"]))

        return d.addCallback(self.assertEquals, [2, "arg"])
