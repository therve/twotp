# -*- test-case-name: twotp.test.test_term -*-
# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Define classes to map erlang types.
"""



class Term(object):
    """
    Simple bean class to represent erlang objects that don't map to python
    objects.
    """



def Integer(val):
    """
    Fake function for mapping erlang integers.
    """
    return val



def Float(val):
    """
    Fake function for mapping erlang floats.
    """
    return val



class NewFloat(Term):
    """
    Wrapper around floats to aim at serialized new floats.
    """

    def __init__(self, value):
        self.value = value



def String(val):
    """
    Fake function for mapping erlang string.
    """
    return val



def Binary(val):
    """
    Fake function for mapping erlang binary data.
    """
    return val



class BitBinary(Term):
    """
    Represent binary data with a number of bits not multiple of 8.
    """

    def __init__(self, data, bits):
        """
        @param data: the full data.
        @type data: C{str}

        @param bits: number of significant bits.
        @type bits: C{int}
        """
        self.data = data
        self.bits = bits


    def __eq__(self, other):
        """
        Check for equality of data and bits.
        """
        if not isinstance(other, BitBinary):
            return False
        return self.data == other.data and self.bits == other.bits


    def __repr__(self):
        """
        Simple representation of the data.
        """
        s =  "<%s at %s> with data %s, bits %s" % (
                self.__class__.__name__, hex(id(self)), self.data, self.bits)
        return s



class AtomNotInCache(KeyError):
    """
    Exception raised if trying to access an atom not in cache.
    """



class Atom(Term):
    """
    Represent an atom term.

    @cvar _cache: cache of atom value.
    @type _cache: C{dict}
    """
    _cache = {}

    def __init__(self, text, cacheIndex=None):
        """
        Initialize the atom and manipulate the cache if necessary.
        """
        self.text = text
        if text is None and cacheIndex is not None:
            if cacheIndex in self._cache:
                self.text = self._cache[cacheIndex]
            else:
                raise AtomNotInCache(cacheIndex)
        elif text is not None and cacheIndex is not None:
            self._cache[cacheIndex] = text


    def __eq__(self, other):
        """
        Check for equality of atom text.
        """
        if not isinstance(other, Atom):
            return False
        return self.text == other.text


    def __repr__(self):
        """
        Simple representation with the text.
        """
        s =  "<%s at %s, text %r>" % (
                self.__class__.__name__, hex(id(self)), self.text)
        return s


    def __hash__(self):
        """
        Build a custom hash, made for giving same hash to Atom with same value.
        """
        return hash((self.__class__, self.text))



def Tuple(val):
    """
    Fake function for mapping erlang tuple.
    """
    return tuple(val)



def List(val):
    """
    Fake function for mapping erlang list.
    """
    return list(val)



def Dict(val):
    """
    Build a dict for key/value pairs.
    """
    return dict(val)



def Set(val):
    """
    Build a set of values.
    """
    return set(val)



class Pid(Term):
    """
    Represent a Pid term.
    """

    def __init__(self, nodeName, nodeId, serial, creation):
        """
        Initialize attributes.
        """
        self.nodeName = nodeName
        self.nodeId = nodeId
        self.serial = serial
        self.creation = creation
        self._links = set()
        self._remoteMonitors = set()
        self._handlers = {}
        self._monitorHandlers = {}


    def __eq__(self, other):
        """
        Check for equality with another Pid.
        """
        if not isinstance(other, Pid):
            return False
        return (self.nodeName == other.nodeName and self.nodeId == other.nodeId
                and self.serial == other.serial
                and self.creation == other.creation)


    def __repr__(self):
        """
        Simple representation with all attributes.
        """
        s =  "<%s at %s, named %r, id %s, serial %s, creation %s>" % (
                self.__class__.__name__, hex(id(self)), self.nodeName,
                self.nodeId, self.serial, self.creation)
        return s


    def __hash__(self):
        """
        Build a custom hash.
        """
        return hash((self.__class__, self.nodeName, self.nodeId, self.serial,
                     self.creation))


    def link(self, proto, pid):
        """
        Link this process to another (remote) C{pid}.
        """
        self._links.add((proto, pid))


    def unlink(self, proto, pid):
        """
        Remove a previously created link to remove process C{pid}.
        """
        if (proto, pid) in self._links:
            self._links.remove((proto, pid))


    def _remoteMonitor(self, proto, pid, ref):
        """
        Register a monitoring by remote process C{pid}. This method should not
        be called by application code, it's a reflect of a call from an Erlang
        node.
        """
        self._remoteMonitors.add((proto, pid, ref))


    def _remoteDemonitor(self, proto, pid, ref):
        """
        Unregister a previous monitoring operation. This method should not be
        called by application code, it's a reflect of a call from an Erlang
        node.
        """
        if (proto, pid, ref) in self._remoteMonitors:
            self._remoteMonitors.remove((proto, pid, ref))


    def exit(self, reason):
        """
        Exit this local process, propagating the exit signal to remote linked
        processes.
        """
        self._links.clear()
        self._remoteMonitors.clear()


    def _signalExit(self, pid, reason):
        """
        Signal remote exit of process C{pid}.
        """
        for handler in self._handlers.pop(pid, []):
            handler(reason)
        for proto, linkedPid in list(self._links):
            if linkedPid == pid:
                self._links.remove((proto, linkedPid))


    def _signalMonitorExit(self, pid, ref, reason):
        """
        Signal remote exit of process C{pid} via monitor.
        """
        for handler in self._monitorHandlers.pop(ref, []):
            handler(reason)



class Reference(Term):
    """
    Represent a Reference term.
    """

    def __init__(self, nodeName, refIds, creation):
        """
        Initialize attributes.
        """
        self.nodeName = nodeName
        if isinstance(refIds, list):
            refIds = tuple(refIds)
        self.refIds = refIds
        self.creation = creation


    def __eq__(self, other):
        """
        Check for equality with another Reference.
        """
        if not isinstance(other, Reference):
            return False
        return (self.nodeName == other.nodeName and self.refIds == other.refIds
                and self.creation == other.creation)


    def __repr__(self):
        """
        Simple representation with all attributes.
        """
        s =  "<%s at %s> named %s, creation %s, ids %r" % (
                self.__class__.__name__, hex(id(self)), self.nodeName,
                self.creation, self.refIds)
        return s


    def __hash__(self):
        """
        Build a custom hash.
        """
        return hash((self.__class__, self.nodeName, self.creation, self.refIds))



class Port(Term):
    """
    Represent a Port term.
    """

    def __init__(self, nodeName, portId, creation):
        """
        Initialize attributes.
        """
        self.nodeName = nodeName
        self.portId = portId
        self.creation = creation


    def __eq__(self, other):
        """
        Check for equality with another Port, checking all attributes.
        """
        if not isinstance(other, Port):
            return False
        return (self.nodeName == other.nodeName and self.portId == other.portId
                and self.creation == other.creation)


    def __repr__(self):
        """
        Simple representation with all attributes.
        """
        s =  "<%s at %s> named %s, creation %s, id %r" % (
                self.__class__.__name__, hex(id(self)), self.nodeName,
                self.creation, self.portId)
        return s



class Fun(Term):
    """
    Represent a Fun term.
    """

    def __init__(self, pid, module, index, uniq, freeVars):
        """
        Initialize attributes.
        """
        self.pid = pid
        self.module = module
        self.index = index
        self.uniq = uniq
        self.freeVars = freeVars


    def __eq__(self, other):
        """
        Check for equality with another Fun, checking all attributes.
        """
        if not isinstance(other, Fun):
            return False
        return (self.pid == other.pid and self.module == other.module and
                self.index == other.index and self.uniq == other.uniq and
                self.freeVars == other.freeVars)


    def __repr__(self):
        """
        Simple representation with all attributes.
        """
        s =  ("<%s at %s> with pid %s, module %s, index %s , uniq %s, "
              "freeVars %s" % (
                self.__class__.__name__, hex(id(self)), self.pid, self.module,
                self.index, self.uniq, self.freeVars))
        return s



class NewFun(Term):
    """
    Represent a NewFun term.
    """

    def __init__(self, pid, module, index, uniq, arity, numFree, oldIndex,
                 oldUniq, freeVars):
        """
        Initialize attributes.
        """
        self.pid = pid
        self.module = module
        self.index = index
        self.uniq = uniq
        self.arity = arity
        self.numFree = numFree
        self.oldIndex = oldIndex
        self.oldUniq = oldUniq
        self.freeVars = freeVars


    def __eq__(self, other):
        """
        Check for equality with another NewFun, checking all attributes.
        """
        if not isinstance(other, NewFun):
            return False
        return (self.pid == other.pid and self.module == other.module and
                self.index == other.index and self.uniq == other.uniq and
                self.arity == other.arity and self.numFree == other.numFree and
                self.oldIndex == other.oldIndex and
                self.oldUniq == other.oldUniq and
                self.freeVars == other.freeVars)


    def __repr__(self):
        """
        Simple representation with all attributes.
        """
        s =  ("<%s at %s> with pid %s, module %s, index %s , uniq %s, "
              "freeVars %s, arity %s, numFree %s, oldIndex %s, oldUniq %s" % (
                self.__class__.__name__, hex(id(self)), self.pid, self.module,
                self.index, self.uniq, self.freeVars, self.arity, self.numFree,
                self.oldIndex, self.oldUniq))
        return s



class Export(Term):
    """
    Represent an Export term.
    """

    def __init__(self, module, function, arity):
        """
        Initialize attributes.
        """
        self.module = module
        self.function = function
        self.arity = arity


    def __eq__(self, other):
        """
        Check for equality with another Export, checking all attributes.
        """
        if not isinstance(other, Export):
            return False
        return (self.module == other.module and
                self.function == other.function and self.arity == other.arity)


    def __repr__(self):
        """
        Simple representation with all attributes.
        """
        s =  ("<%s at %s> function %s in module %s, arity %s" % (
                self.__class__.__name__, hex(id(self)), self.module,
                self.function, self.arity))
        return s



class ConstantHolder(object):
    """
    Bean easing magic constants export.
    """
    MAGIC_VERSION = 131
    MAGIC_STRING = 107
    MAGIC_NIL = 106
    MAGIC_LIST = 108
    MAGIC_SMALL_TUPLE = 104
    MAGIC_LARGE_TUPLE = 105
    MAGIC_LARGE_BIG = 111
    MAGIC_SMALL_BIG = 110
    MAGIC_FLOAT = 99
    MAGIC_SMALL_INTEGER = 97
    MAGIC_INTEGER = 98
    MAGIC_ATOM = 100
    MAGIC_NEW_REFERENCE = 114
    MAGIC_REFERENCE = 101
    MAGIC_PORT = 102
    MAGIC_PID = 103
    MAGIC_BINARY = 109
    MAGIC_FUN = 117
    MAGIC_NEW_FUN = 112
    MAGIC_NEW_CACHE = 78
    MAGIC_CACHED_ATOM = 67
    MAGIC_EXPORT = 113
    MAGIC_BIT_BINARY = 77
    MAGIC_COMPRESSED = 80
    MAGIC_NEW_FLOAT = 70



class Node(object):
    """
    Bean holding information about an erlang node.
    """

    def __init__(self, portNumber, nodeType, protocol, distrVSNRng, nodeName,
                 extra):
        """
        Iniatialize Node attributes.
        """
        self.portNumber = portNumber
        self.nodeType = nodeType
        self.protocol = protocol
        self.distrVSNRng = distrVSNRng
        self.nodeName = nodeName
        self.extra = extra


    def __eq__(self, other):
        """
        Check equality with another node.
        """
        if not isinstance(other, Node):
            return False
        return (self.portNumber == other.portNumber and
                self.nodeType == other.nodeType and
                self.protocol == other.protocol and
                self.distrVSNRng == other.distrVSNRng and
                self.nodeName == other.nodeName and self.extra == other.extra)
