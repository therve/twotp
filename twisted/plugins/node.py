# Copyright (c) 2007-2008 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Twisted erlang node.
"""


from zope.interface import implements

from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker
from twisted.python.usage import Options
from twisted.python.reflect import namedAny

from twotp.epmd import PersistentPortMapperService
from twotp import readCookie, buildNodeName



class NodeOptions(Options):
    optParameters = [
        ['nodename', 'n', 'twisted', 'Name of the node'],
        ['cookie', 'c', '', 'Erlang cookie value'],
        ['methods', 'm', '', 'Methods holder']
    ]



class NodePlugin(object):
    implements(IPlugin, IServiceMaker)

    tapname = "twotp"
    description = "twotp erlang node"
    options = NodeOptions


    def makeService(self, options):
        cookie = options['cookie']
        if not cookie:
            cookie = readCookie()
        methodsHolder = options.get('methods', {})
        if methodsHolder:
            methodsHolder = namedAny(methodsHolder)
        nodeName = buildNodeName(options['nodename'])
        epmd = PersistentPortMapperService(methodsHolder, nodeName, cookie)
        return epmd



nodePlugin = NodePlugin()
