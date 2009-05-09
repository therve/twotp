#!/usr/bin/env python
# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
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

from twotp import Process, readCookie, buildNodeName, Atom



class MnesiaManager(object):
    def __init__(self, process):
        self.process = process
        self.tables = []

        self.window = gtk.Window(gtk.WINDOW_TOPLEVEL)
        self.window.connect("delete_event", self.delete_event)
        self.window.connect("destroy", self.destroy)
        self.window.set_border_width(10)
        self.window.set_size_request(800, 500)

        self.scrolled_window = gtk.ScrolledWindow()
        self.scrolled_window.set_border_width(0)
        self.scrolled_window.set_policy(gtk.POLICY_AUTOMATIC, gtk.POLICY_ALWAYS)

        self.vbox = gtk.VBox(False, 0)
        self.window.add(self.vbox)

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

        self.setup_menu()

        self.vbox.pack_start(self.menu_bar, expand=False, fill=False)
        self.vbox.pack_start(self.scrolled_window, expand=True, fill=True)

        self.menu_bar.show()
        self.listview.show()
        self.scrolled_window.show()
        self.vbox.show()
        self.window.show()

        self.gotConnection()


    def setup_menu(self):
        self.menu_bar = gtk.MenuBar()
        table_menu = gtk.Menu()
        self.list_tables_menu = gtk.Menu()
        list_item = gtk.MenuItem("List")
        quit_item = gtk.MenuItem("Quit")

        table_menu.append(list_item)
        table_menu.append(quit_item)

        list_item.set_submenu(self.list_tables_menu)

        quit_item.connect_object ("activate", self.destroy, "file.quit")

        list_item.show()
        quit_item.show()

        table_item = gtk.MenuItem("Tables")
        table_item.show()

        table_item.set_submenu(table_menu)

        self.menu_bar.append(table_item)


    def list_table(self, table):
        d = self.process.callRemote("twisted_mnesia", "mnesia", "table_info", table,
                Atom("all"))

        def cb(result):
            self.liststore.clear()
            for name, value in result:
                if isinstance(value, Atom):
                    self.liststore.append((name.text, value.text))
                else:
                    self.liststore.append((name.text, str(value)))
        return d.addCallback(cb)


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
        self.tables = tables
        for table in tables:
            table_item = gtk.MenuItem(table.text)
            self.list_tables_menu.append(table_item)
            table_item.connect_object("activate", self.list_table, table)
            table_item.show()

        return ", ".join([t.text for t in tables])


    def hook_running_db_nodes(self, nodes):
        return ", ".join([n.text for n in nodes])


    def hook_subscribers(self, pids):
        return ", ".join(["%s at %s" % (p.nodeId, p.nodeName.text) for p in pids])


    def gotConnection(self):
        def cb(result):
            for name, value in result:
                if isinstance(value, Atom):
                    self.liststore.append((name.text, value.text))
                else:
                    meth = getattr(self, "hook_%s" % (name.text,), None)
                    if meth is not None:
                        self.liststore.append((name.text, meth(value)))
                    else:
                        self.liststore.append((name.text, str(value)))
        return self.process.callRemote("twisted_mnesia", "mnesia",
                "system_info", Atom("all")).addCallback(cb)



def main():
    cookie = readCookie()
    nodeName = buildNodeName('nodename')
    process = Process(nodeName, cookie)
    manager = MnesiaManager(process)


if __name__ == "__main__":
    reactor.callWhenRunning(main)
    reactor.run()
