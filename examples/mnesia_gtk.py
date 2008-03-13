#!/usr/bin/env python
# Copyright (c) 2007-2008 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
GTK interface showing information about a mnesia database.
"""

import pygtk
pygtk.require('2.0')
import gtk

from twisted.internet import gtk2reactor
gtk2reactor.install()

from twisted.internet import reactor

from erlang import OneShotPortMapperFactory, readCookie, buildNodeName, Atom



class MnesiaManager(object):
    def __init__(self, epmd):
        self.epmd = epmd

        self.window = gtk.Window(gtk.WINDOW_TOPLEVEL)
        self.window.connect("delete_event", self.delete_event)
        self.window.connect("destroy", self.destroy)
        self.window.set_border_width(10)
        self.window.set_size_request(800, 500)

        self.scrolled_window = gtk.ScrolledWindow()
        self.scrolled_window.set_border_width(0)
        self.scrolled_window.set_policy(gtk.POLICY_AUTOMATIC, gtk.POLICY_ALWAYS)
        self.window.add(self.scrolled_window)

        self.liststore = gtk.ListStore(str, str)

        self.listview = gtk.TreeView(self.liststore)
        self.namecolumn = gtk.TreeViewColumn('Name')
        self.valuecolumn = gtk.TreeViewColumn('Value')
        self.listview.append_column(self.namecolumn)
        self.listview.append_column(self.valuecolumn)

        self.namecell = gtk.CellRendererText()
        self.namecolumn.pack_start(self.namecell, True)
        self.namecolumn.add_attribute(self.namecell, 'text', 0)

        self.valuecell = gtk.CellRendererText()
        self.valuecolumn.pack_start(self.valuecell, True)
        self.valuecolumn.add_attribute(self.valuecell, 'text', 1)

        self.scrolled_window.add_with_viewport(self.listview)

        self.listview.show()
        self.scrolled_window.show()
        self.window.show()

        self.epmd.connectToNode("twisted_mnesia").addCallback(self.gotConnection)


    def destroy(self, widget, data=None):
        reactor.stop()


    def delete_event(self, widget, event, data=None):
        return False


    def hook_db_nodes(self, nodes):
        return ", ".join([n.text for n in nodes])

    def hook_fallback_error_function(self, fct):
        return "%s:%s" % (fct[0].text, fct[1].text)


    def hook_local_tables(self, tables):
        return ", ".join([t.text for t in tables])


    def hook_tables(self, tables):
        return ", ".join([t.text for t in tables])


    def hook_running_db_nodes(self, nodes):
        return ", ".join([n.text for n in nodes])


    def hook_subscribers(self, pids):
        return ", ".join(["%s at %s" % (p.nodeId, p.nodeName.text) for p in pids])


    def gotConnection(self, proto):
        self.proto = proto
        def cb(result):
            for i in result:
                name = i[0].text
                if len(i) == 2 and isinstance(i[1], Atom):
                    self.liststore.append((name, i[1].text))
                else:
                    meth = getattr(self, "hook_%s" % (name,), None)
                    if meth is not None:
                        self.liststore.append((name, meth(i[1])))
                    else:
                        self.liststore.append((name, str(i[1])))
        return proto.factory.callRemote(proto, "mnesia", "system_info", 
                Atom("all")).addCallback(cb)



def main():
    cookie = readCookie()
    nodeName = buildNodeName('nodename')
    epmd = OneShotPortMapperFactory(nodeName, cookie)
    manager = MnesiaManager(epmd)


if __name__ == "__main__":
    reactor.callWhenRunning(main)
    reactor.run()