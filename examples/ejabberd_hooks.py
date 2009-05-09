#!/usr/bin/env python
"""
Example connecting to an ejabberd node to get notifications. Written by Fabio
Forno from Bluendo, originally published here:

http://blog.bluendo.com/ff/ejabberd-hooks-with-twisted

"""

from twisted.internet import reactor, defer

from twotp import Process, Atom, readCookie

from pprint import pprint

from twisted.words.xish import domish


def to_domish(el):
    """
    Make a domish elememt from a ejabberd xml element
    """
    node = domish.Element((None, l2u(el[1])))
    for k, v in el[2]:
        node[l2u(k)] = l2u(v)
    for child in el[3]:
        if child[0].text == "xmlelement":
            node.addChild(to_domish(child))
        else:
            node.addContent(child[1].decode("utf-8"))
    return node


def l2u(l):
    return ''.join([chr(i) for i in l]).decode("utf-8")


@defer.inlineCallbacks
def register_hook(process):

    r = yield process.callRemote(
            "ejabberd",
            "ejabberd_hooks",
            "add_dist",
            Atom("user_receive_packet"), # hook name
            "olindo.bluendo.priv", # virtual host
            Atom("uccaro@olindo.bluendo.priv"), # hook node
            Atom("proxy"), # hook module
            Atom("receive_packet"), # hook method
            10
    )

    r = yield process.callRemote(
            "ejabberd",
            "ejabberd_hooks",
            "add_dist",
            Atom("user_send_packet"), # hook name
            "olindo.bluendo.priv", # virtual host
            Atom("uccaro@olindo.bluendo.priv"), # hook node
            Atom("proxy"), # hook module
            Atom("send_packet"), # hook method
            10
    )


class UserMonitor(object):

    def remote_user_available(self, jid):
        # do something interesting
        print "user %s available"%(l2u(jid[1]))
        return Atom("ok")

    def remote_set_presence(self, user, server, resource, p):
        pprint(p)
        print "new presence: %s %s %s"%(l2u(user), l2u(server), l2u(resource))
        print to_domish(p).toXml()
        return Atom("ok")

    def remote_receive_packet(self, jid, frm, to, packet):
        print ">>",  to_domish(packet).toXml()

    def remote_send_packet(self, frm, to, packet):
        print "<<",  to_domish(packet).toXml()


if __name__ == "__main__":
    cookie = readCookie()

    nodeName = "controller@olindo.bluendo.priv"
    process1 = Process(nodeName, cookie, "olindo.bluendo.priv")
    reactor.callWhenRunning(register_hook, process1)

    nodeName = "uccaro@olindo.bluendo.priv"
    process2 = Process(nodeName, cookie, "olindo.bluendo.priv")
    process2.registerModule("proxy", UserMonitor())
    process2.listen()

    reactor.run()
