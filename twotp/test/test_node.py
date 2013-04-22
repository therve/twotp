# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Test basic node functionalities.
"""

from twisted.internet.task import Clock
from twisted.test.proto_helpers import StringTransportWithDisconnection

from twotp.node import (
    NodeProtocol, buildNodeName, getHostName, MessageHandler, Process,
    NetKernelProcess, SpawnProcess)
from twotp.server import NodeServerFactory
from twotp.term import Pid, Atom, Reference, Tuple
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


    def randomFactory(self, a, b):
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
        self.assertEqual(called, [])
        self.proto.clock.advance(1)
        self.assertEqual(called, [""])


    def test_sendHandshake(self):
        """
        Test a send during the C{handshake} state.
        """
        self.proto.state = "handshake"
        self.proto.send("foo")
        self.assertEqual(self.transport.value(), "\x00\x03foo")


    def test_sendChallenge(self):
        """
        Test a send during the C{challenge} state.
        """
        self.proto.state = "challenge"
        self.proto.send("bar")
        self.assertEqual(self.transport.value(), "\x00\x03bar")


    def test_sendConnected(self):
        """
        Test a send during the C{connected} state.
        """
        self.proto.state = "connected"
        self.proto.send("egg")
        self.assertEqual(self.transport.value(), "\x00\x00\x00\x03egg")


    def test_generateDigest(self):
        """
        Test output value of generateDigest.
        """
        self.assertEqual(self.proto.generateDigest(123, "test_cookie"),
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
        self.assertEqual(calls, [(self.proto, (1,))])



class UtilitiesTestCase(TestCase):
    """
    Tests for utilities functions.
    """

    def test_buildNodeName(self):
        """
        Tests for C{buildNodeName}.
        """
        nodeName = buildNodeName("foo@bar")
        self.assertEqual(nodeName, "foo@bar")
        nodeName = buildNodeName("foo")
        self.assertIn("@", nodeName)


    def test_getHostName(self):
        """
        Tests for C{getHostName}: it returns a non empty string.
        """
        hostName = getHostName()
        self.assertIsInstance(hostName, str)
        self.assertNotEqual(hostName, "")



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
        self.assertEqual(destPid._links, set([(None, srcPid)]))


    def test_unlinkNotExisting(self):
        """
        Test handling of an UNLINK token while the link doesn't exit locally.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        ctrlMessage = (self.handler.CTRLMSGOP_UNLINK, srcPid, destPid)
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEqual(destPid._links, set([]))


    def test_unlink(self):
        """
        Test handling of an UNLINK token.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        destPid.link(None, srcPid)
        # Sanity check
        self.assertNotEqual(destPid._links, set([]))

        ctrlMessage = (self.handler.CTRLMSGOP_UNLINK, srcPid, destPid)
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEqual(destPid._links, set([]))


    def test_nodeLink(self):
        """
        Test handling of a NODE_LINK token.
        """
        self.assertRaises(
            NotImplementedError, self.handler.passThroughMessage, None,
            (self.handler.CTRLMSGOP_NODE_LINK,), None)


    def test_groupLeader(self):
        """
        Test handling of a GROUP_LEADER token.
        """
        self.assertRaises(
            NotImplementedError, self.handler.passThroughMessage, None,
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
        self.assertEqual(called, [("reason",)])
        self.assertEqual(destPid._links, set([]))


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
        self.assertEqual(called, [("reason",)])
        self.assertEqual(destPid._links, set([]))


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
        self.assertEqual(called, [("reason",)])
        self.assertEqual(destPid._links, set([]))


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
        self.assertEqual(called, [("reason",)])
        self.assertEqual(destPid._links, set([]))


    def test_monitorP(self):
        """
        Test handling of a MONITOR_P token.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)
        ctrlMessage = (self.handler.CTRLMSGOP_MONITOR_P, srcPid, destPid, ref)
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEqual(destPid._remoteMonitors, set([(None, srcPid, ref)]))


    def test_demonitorPNotExisting(self):
        """
        Test handling of a DEMONITOR_P token while the link doesn't exist
        locally.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)
        ctrlMessage = (self.handler.CTRLMSGOP_DEMONITOR_P, srcPid, destPid,
                       ref)
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEqual(destPid._remoteMonitors, set([]))


    def test_demonitorP(self):
        """
        Test handling of a DEMONITOR_P token.
        """
        srcPid = Pid(Atom("foo@bar"), 0, 0, 0)
        destPid = Pid(Atom("spam@egg"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)
        destPid._remoteMonitor(None, srcPid, ref)
        # Sanity check
        self.assertNotEqual(destPid._remoteMonitors, set([]))

        ctrlMessage = (self.handler.CTRLMSGOP_DEMONITOR_P, srcPid, destPid,
                       ref)
        self.handler.passThroughMessage(None, ctrlMessage, None)
        self.assertEqual(destPid._remoteMonitors, set([]))


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
        self.assertEqual(called, [("reason",)])


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
        pid = self.handler.createPid()
        self.assertEqual(pid.nodeId, 0)
        self.assertEqual(pid.serial, 0)
        pid = self.handler.createPid()
        self.assertEqual(pid.nodeId, 1)
        self.assertEqual(pid.serial, 0)


    def test_createPidSerialIncrement(self):
        """
        Check that L{MessageHandler.createPid} increments the serial number
        used for pid when the nodeId reaches the 0x7fff value.
        """
        self.handler.pidCount = 0x7fff
        pid = self.handler.createPid()
        self.assertEqual(pid.nodeId, 32767)
        self.assertEqual(pid.serial, 0)

        pid = self.handler.createPid()
        self.assertEqual(pid.nodeId, 0)
        self.assertEqual(pid.serial, 1)


    def test_createPidSerialReset(self):
        """
        Check that L{MessageHandler.createPid} resets the serial number to 0
        when the nodeId reaches the 0x7fff value and the serial value reaches
        the 0x1fff value.
        """
        self.handler.pidCount = 0x7fff
        self.handler.serial = 0x1fff
        pid = self.handler.createPid()
        self.assertEqual(pid.nodeId, 32767)
        self.assertEqual(pid.serial, 8191)

        pid = self.handler.createPid()
        self.assertEqual(pid.nodeId, 0)
        self.assertEqual(pid.serial, 0)


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
        self.assertEqual(pid.nodeId, 32767)
        self.assertEqual(pid.serial, 7)

        pid = self.handler.createPid()
        self.assertEqual(pid.nodeId, 0)
        self.assertEqual(pid.serial, 0)


    def test_createPort(self):
        """
        Test L{MessageHandler.createPort}.
        """
        port = self.handler.createPort()
        self.assertEqual(port.portId, 0)

        port = self.handler.createPort()
        self.assertEqual(port.portId, 1)


    def test_createPortReset(self):
        """
        L{MessageHandler.createPort} resets the port value to 0 when it reaches
        the 0xfffffff value.
        """
        self.handler.portCount = 0xfffffff
        port = self.handler.createPort()
        self.assertEqual(port.portId, 268435455)

        port = self.handler.createPort()
        self.assertEqual(port.portId, 0)


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
        self.assertEqual(port.portId, 262143)

        port = self.handler.createPort()
        self.assertEqual(port.portId, 0)



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
    oneShotPortMapperClass = TestableProcessOSMPF

    persistentPortMapperClass = TestablePPMF



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
        d = self.process.listen()
        self.process.persistentEpmd._connectDeferred.callback(2)

        def check(ignored):
            factory = self.process.serverFactory
            handler = factory.handler
            self.assertIsInstance(factory, NodeServerFactory)
            self.assertIdentical(handler, self.process.handler)
            rexProcess = handler._namedProcesses["rex"].im_self
            self.assertEqual(factory, rexProcess.serverFactory)
            kernelProcess = handler._namedProcesses["net_kernel"].im_self
            self.assertEqual(factory, rexProcess.serverFactory)


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
        self.assertEqual(transport.value(), "\x00\x04zegg")
        self.assertEqual(epmd.connect, [("spam", 4369, epmd)])
        proto.dataReceived(
            "w\x00\x00\x09M\x01\x00\x05\x00\x05\x00\x03bar\x00")

        [factory] = epmd.factories
        self.assertEqual(
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
        self.assertEqual(
            transport.value(),
            "\x00\x00\x00\x7fp\x83h\x04a\x06gd\x00\x07foo@bar\x00"
            "\x00\x00\x03\x00\x00\x00\x00\x00d\x00\x00d\x00\nnet_kernel\x83h"
            "\x03d\x00\t$gen_callh\x02gd\x00\x07foo@bar\x00\x00\x00\x03\x00"
            "\x00\x00\x00\x00r\x00\x03d\x00\x07foo@bar\x00\x00\x00\x00\x01"
            "\x00\x00\x00\x00\x00\x00\x00\x00h\x02d\x00\x07is_authd\x00\x07"
            "foo@bar")
        pid = list(set(self.process.handler._parser._pids) - pids)[0]
        ref = Reference(Atom("foo@bar"), 0, 0)
        yes = Atom("yes")

        self.process.handler.operation_send(proto, (Atom(""), pid), (ref, yes))
        return d.addCallback(self.assertEqual, "pong")


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
        self.assertEqual(
            transport.value(),
            "\x00\x00\x00jp\x83h\x04a\x06gd\x00\x07foo@bar\x00\x00"
            "\x00\x03\x00\x00\x00\x00\x00d\x00\x00d\x00\x03rex\x83h\x02gd\x00"
            "\x07foo@bar\x00\x00\x00\x03\x00\x00\x00\x00\x00h\x05d\x00\x04"
            "calld\x00\x07module1d\x00\x05func1l\x00\x00\x00\x02k\x00\x03arga"
            "\x01jd\x00\x04user")
        pid = list(set(self.process.handler._parser._pids) - pids)[0]

        self.process.handler.operation_send(
            proto, (Atom(""), pid), (Atom("rex"), [2, "arg"]))

        return d.addCallback(self.assertEqual, [2, "arg"])


    def test_names(self):
        """
        L{Process.names} returns the list of nodes on a particular host.
        """
        d = self.process.names("spam")
        epmd = self.process.oneShotEpmds["spam"]
        transport = StringTransportWithDisconnection()
        proto = epmd.buildProtocol(("127.0.01", 4369))
        proto.makeConnection(transport)
        transport.protocol = proto
        self.assertEqual(transport.value(), "\x00\x01n")
        proto.dataReceived("\x00\x00\x00\x01")
        proto.dataReceived("name %s at port %s\n" % ("foo", 1234))
        proto.dataReceived("name %s at port %s\n" % ("egg", 4321))
        transport.loseConnection()
        return d.addCallback(self.assertEqual, [("foo", 1234), ("egg", 4321)])



class TestableNetKernelProcess(NetKernelProcess):
    """
    A testable version of L{NetKernelProcess}.
    """
    oneShotPortMapperClass = TestableProcessOSMPF

    persistentPortMapperClass = TestablePPMF



class NetKernelProcessTestCase(TestCase):
    """
    Tests for L{NetKernelProcess}.
    """

    def setUp(self):
        self.clock = Clock()
        self.factory = DummyFactory()
        self.process = TestableNetKernelProcess("foo@bar", "test_cookie",
                                                self.factory.handler, {})
        self.process.callLater = self.clock.callLater

        proto = TestableNodeProtocol()
        proto.state = "connected"
        transport = CloseNotifiedTransport()
        proto.factory = self.factory
        proto.makeConnection(transport)
        transport.protocol = proto

        self.transport = transport
        self.protocol = proto


    def test_ping(self):
        """
        L{NetKernelProcess} is able to answer C{ping} requests with C{yes}.
        """
        pid = Pid(Atom("foo@bar"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)
        msg = Tuple((Atom("$gen_call"), Tuple((pid, ref)),
                     Tuple((Atom("is_auth"), Atom("egg@spam")))))
        self.process._receivedData(self.protocol, None, msg)

        self.assertEqual(
            "\x00\x00\x007p\x83h\x03a\x02d\x00\x00gd\x00\x07"
            "foo@bar\x00\x00\x00\x00\x00\x00\x00\x00\x00\x83h"
            "\x02ed\x00\x08spam@egg\x00\x00\x00\x00\x00d\x00\x03yes",
            self.transport.value())


    def test_spawn(self):
        """
        L{NetKernelProcess} answers C{spawn} calls with the pid of the created
        process, create the related L{SpawnProcess} instance and calls its
        C{start} method.
        """
        started = []

        class TestProcess(SpawnProcess):

            def start(self, pid, args):
                started.append((pid, args))

        class Holder(object):

            func = TestProcess

        self.process._methodsHolder["mod"] = Holder()

        pid = Pid(Atom("foo@bar"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)
        msg = Tuple((Atom("$gen_call"), Tuple((pid, ref)),
                     Tuple((Atom("spawn"), Atom("mod"), Atom("func"),
                            (Atom("ok"), "args")))))
        self.process._receivedData(self.protocol, None, msg)

        self.assertEqual(
            "\x00\x00\x00Fp\x83h\x03a\x02d\x00\x00gd\x00\x07foo@bar"
            "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x83h\x02ed\x00\x08"
            "spam@egg\x00\x00\x00\x00\x00gd\x00\x08spam@egg\x00\x00"
            "\x00\x01\x00\x00\x00\x00\x00",
            self.transport.value())

        self.assertEqual([(pid, (Atom("ok"), "args"))], started)


    def test_spawnUnknownModule(self):
        """
        L{NetKernelProcess} answers C{spawn} calls with an error message if the
        specified module can't be found.
        """
        pid = Pid(Atom("foo@bar"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)
        msg = Tuple((Atom("$gen_call"), Tuple((pid, ref)),
                     Tuple((Atom("spawn"), Atom("mod"), Atom("func"),
                            (Atom("ok"), "args")))))
        self.process._receivedData(self.protocol, None, msg)

        self.assertEqual(
            "\x00\x00\x00Jp\x83h\x03a\x02d\x00\x00gd\x00\x07foo@bar"
            "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x83h\x02ed\x00\x08"
            "spam@egg\x00\x00\x00\x00\x00d\x00\x16undefined module 'mod'",
            self.transport.value())


    def test_spawnUnknownProcess(self):
        """
        L{NetKernelProcess} answers C{spawn} calls with an error message if the
        speficied function can't be found.
        """
        class Holder(object):
            pass

        self.process._methodsHolder["mod"] = Holder()

        pid = Pid(Atom("foo@bar"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)
        msg = Tuple((Atom("$gen_call"), Tuple((pid, ref)),
                     Tuple((Atom("spawn"), Atom("mod"), Atom("func"),
                            (Atom("ok"), "args")))))
        self.process._receivedData(self.protocol, None, msg)

        self.assertEqual(
            "\x00\x00\x00Mp\x83h\x03a\x02d\x00\x00gd\x00\x07foo@bar"
            "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x83h\x02ed\x00\x08"
            "spam@egg\x00\x00\x00\x00\x00d\x00\x19undefined function 'func'",
            self.transport.value())


    def test_spawnNonProcess(self):
        """
        L{NetKernelProcess} answers C{spawn} calls with an error message if the
        speficied object is a not a L{SpawnProcess} subclass.
        """
        class Holder(object):
            func = "Func"

        self.process._methodsHolder["mod"] = Holder()

        pid = Pid(Atom("foo@bar"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)
        msg = Tuple((Atom("$gen_call"), Tuple((pid, ref)),
                     Tuple((Atom("spawn"), Atom("mod"), Atom("func"),
                            (Atom("ok"), "args")))))
        self.process._receivedData(self.protocol, None, msg)

        self.assertEqual(
            "\x00\x00\x00Hp\x83h\x03a\x02d\x00\x00gd\x00\x07foo@bar"
            "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x83h\x02ed\x00\x08"
            "spam@egg\x00\x00\x00\x00\x00d\x00\x14wrong process 'func'",
            self.transport.value())


    def test_spawnLink(self):
        """
        L{NetKernelProcess} handles C{spawn_link} calls by creating the asked
        process and making a link call.
        """
        started = []

        class TestProcess(SpawnProcess):

            def start(self, pid, args):
                started.append((pid, args))

        class Holder(object):

            func = TestProcess

        self.process._methodsHolder["mod"] = Holder()

        pid = Pid(Atom("foo@bar"), 0, 0, 0)
        ref = Reference(Atom("spam@egg"), 0, 0)
        msg = Tuple((Atom("$gen_call"), Tuple((pid, ref)),
                     Tuple((Atom("spawn_link"), Atom("mod"), Atom("func"),
                            (Atom("ok"), "args")))))
        self.process._receivedData(self.protocol, None, msg)

        self.assertEqual(
            "\x00\x00\x00/p\x83h\x03a\x01gd\x00\x08spam@egg"
            "\x00\x00\x00\x01\x00\x00\x00\x00\x00gd\x00\x07foo@bar"
            "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Fp\x83h\x03a\x02"
            "d\x00\x00gd\x00\x07foo@bar\x00\x00\x00\x00\x00\x00\x00\x00\x00"
            "\x83h\x02ed\x00\x08spam@egg\x00\x00\x00\x00\x00gd\x00\x08"
            "spam@egg\x00\x00\x00\x01\x00\x00\x00\x00\x00",
            self.transport.value())

        self.assertEqual([(pid, (Atom("ok"), "args"))], started)
