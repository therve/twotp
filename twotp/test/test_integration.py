# Copyright (c) 2007-2010 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Integration tests with a real erlang process.
"""

import os

from twisted.internet import reactor
from twisted.internet.error import ProcessDone
from twisted.internet.defer import Deferred
from twisted.internet.protocol import ProcessProtocol
from twisted.python.procutils import which
from twisted.trial.unittest import SkipTest

from twotp import Process, buildNodeName, SpawnProcess
from twotp.term import Atom, Tuple
from twotp.test.util import TestCase



class ErlangProcessProtocol(ProcessProtocol):
    """
    A simple process protocol, with capturing capabilities suitable for
    tests.

    @ivar onConnection: L{Deferred} fired when the connection has been
        established with the created process.
    @ivar onCompletion: L{Deferred} fired when the connection has been lost
        with the created process.
    @ivar onDataReceived: if specified, L{Deferred} fired at the next
        C{dataReceived} call.
    """

    onDataReceived = None


    def __init__(self):
        self.onConnection = Deferred()
        self.onCompletion = Deferred()
        self.data = {}


    def connectionMade(self):
        self.onConnection.callback(None)


    def childDataReceived(self, name, bytes):
        self.data[name] = self.data.get(name, '') + bytes
        if self.onDataReceived is not None:
            d, self.onDataReceived = self.onDataReceived, None
            d.callback(self)


    def processEnded(self, reason):
        self.onCompletion.callback(reason)



class IntegrationTestCase(TestCase):

    def setUp(self):
        self.protocol = ErlangProcessProtocol()
        self.protocol.onDataReceived = Deferred()
        executables = which("erl")
        if not executables:
            raise SkipTest("No erl process")
        self.cookie = "twotp_cookie"
        self.erlangName = "twotp_erlang_test@localhost"
        self.nodeName = "twotp_python_test@localhost"
        args = [executables[0], "-setcookie", self.cookie, "-sname",
                self.erlangName]
        self.process = reactor.spawnProcess(
            self.protocol, executables[0], args, env=dict(os.environ))
        # We wait for input to be available
        return self.protocol.onDataReceived


    def tearDown(self):
        self.protocol.transport.write("q().\n")
        return self.assertFailure(self.protocol.onCompletion, ProcessDone)


    def test_ping(self):
        """
        L{Process.ping} returns a L{Deferred} which fires with "pong" if the
        process was found.
        """
        process = Process(self.nodeName, self.cookie)

        def check(response):
            self.assertEqual(response, "pong")

        return process.ping(self.erlangName).addCallback(check)


    def test_callRemote(self):
        """
        L{Process.callRemote} executes the given function on the remove process
        and returns the reponse.
        """
        process = Process(self.nodeName, self.cookie)

        def check(response):
            expected = (Atom("ok"), map(ord, os.getcwd()))
            self.assertEqual(response, expected)

        return process.callRemote(
            self.erlangName, "file", "get_cwd").addCallback(check)


    def test_dictCompatibility(self):
        """
        Dicts created by twotp are compatible with erlang dict.
        """
        d = {Atom("foo"): "spam"}
        process = Process(self.nodeName, self.cookie)

        def check(response):
            expected = {Atom("egg"): 4, Atom("foo"): map(ord, "spam")}
            self.assertEqual(response, expected)

        deferred = process.callRemote(
            self.erlangName, "dict", "store", Atom("egg"), 4, d)
        return deferred.addCallback(check)


    def test_setCompatibility(self):
        """
        Sets created by twotp are compatible with erlang sets.
        """
        s = set([Atom("foo"), 3])
        process = Process(self.nodeName, self.cookie)

        def check(response):
            expected = set([Atom("foo"), 3, Atom("egg")])
            self.assertEqual(response, expected)

        deferred = process.callRemote(
            self.erlangName, "sets", "add_element", Atom("egg"), s)
        return deferred.addCallback(check)


    def test_spawn(self):
        """
        Spawn a process on the erlang side, send data from python, and receive
        data after.
        """
        d = Deferred()

        class TestProcess(SpawnProcess):

            def start(oself, pid, *args):
                oself.send(pid, Tuple(["ok", "connect"]))
                oself.receive().chainDeferred(d)

        class RemoteCalls(object):
            test = TestProcess

        process = Process(self.nodeName, self.cookie)
        process.registerModule("api", RemoteCalls())

        def spawn(ignore):
            self.protocol.transport.write(
                "Pid = spawn(%s, api, test, []).\n" % self.nodeName)
            self.protocol.transport.write(
                "receive Msg -> io:format(\"**~p**~n\", [Msg]) end.\n")
            self.protocol.transport.write(
                "Pid ! {self(), \"ok\", \"disconnect\"}.\n")

            return d

        def check(result):
            self.assertIn("**{\"ok\",\"connect\"}**", self.protocol.data[1])
            self.assertEqual("ok", "".join(map(chr, result[1])))
            self.assertEqual("disconnect", "".join(map(chr, result[2])))
            return process.stop()

        d.addCallback(check)

        return process.listen().addCallback(spawn)
