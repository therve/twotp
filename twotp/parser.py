# -*- test-case-name: twotp.test.test_parser -*-
# Copyright (c) 2007-2008 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Parsing of data received from erlang node or epmd.
"""

import struct, zlib

from twotp.term import Integer, String, List, Tuple, Float, Atom, Reference
from twotp.term import Port, Pid, Binary, Fun, NewFun, Export, BitBinary
from twotp.term import ConstantHolder, Dict, Set



class UnhandledCode(KeyError):
    """
    Exception raised when trying to parse data with an unknown typecode.
    """


class ImproperListError(ValueError):
    """
    Exception raised when having an improper list.
    """



class RemainingDataError(ValueError):
    """
    Exception raised when parsing a binary stream with data remaining.
    """



class Parser(ConstantHolder):
    """
    Parse binary data from an erlang node into terms.
    """

    def __init__(self):
        """
        Initialize the parser and the mapping used between code on one byte and
        the parsing method.
        """
        ConstantHolder.__init__(self)
        self.mapping = {}
        for name, val in ConstantHolder.__dict__.iteritems():
            if name.startswith('MAGIC_'):
                name = name.split('MAGIC_')[1].lower()
                self.mapping[val] = getattr(self, 'parse_%s' % name)


    def parseChar(self, bytes):
        """
        Parse one byte of data into an int.
        """
        return ord(bytes)


    def parseShort(self, bytes):
        """
        Parse two bytes of data into a short int.
        """
        return struct.unpack("!H", bytes)[0]


    def parseInt(self, bytes):
        """
        Parse four bytes of data into an int.
        """
        return struct.unpack("!I", bytes)[0]


    def parse_version(self, data):
        """
        Parse version shouldn't be called.
        """
        raise RuntimeError("Should not ne here!")


    def parse_compressed(self, data):
        """
        Parse compressed data.
        """
        length = self.parseInt(data[:4])
        uncompressedData = zlib.decompress(data[4:], 15, length)
        if len(uncompressedData) != length:
            raise ValueError("Too big uncompressed data")
        return self.binaryToTerm(uncompressedData)


    def parse_string(self, data):
        """
        Parse a string term.
        """
        strLen = self.parseShort(data[:2])
        strText = data[2:2 + strLen]
        lst = map(ord, strText)
        return List(lst), data[2 + strLen:]


    def parse_nil(self, data):
        """
        Parse nil term.
        """
        return List([]), data


    def parse_list(self, data):
        """
        Parse a list of terms.
        """
        arity = self.parseInt(data[:4])
        elements, data = self._parse_seq(arity, data[4:])
        if len(data) == 0 or ord(data[0]) != self.MAGIC_NIL:
            raise ImproperListError()
        return List(elements), data[1:]


    def _parse_seq(self, arity, data):
        """
        Helper methods to deal with sequence of encoded terms.

        @param arity: number of terms to search for.
        @type arity: C{int}
        @param data: binary data to parse.
        @type data: C{str}

        @return: list of decoded terms, remaining data.
        @rtype: C{tuple}
        """
        res = []
        for i in xrange(arity):
            term, data = self.binaryToTerm(data)
            res.append(term)
        return res, data


    def _identify_dict(self, elements):
        """
        Identify a dictionary from a tuple of elements.
        """
        if elements:
            if isinstance(elements[0], Atom) and elements[0].text == "dict":
                if len(elements) == 9:
                    # 'dict', size, active, max, offset, expand, contract,
                    # empty, content
                    d = []
                    for i in elements[8][0]:
                        if i:
                            d.append((i[0][0], i[0][1]))
                    return d
        return None


    def _identify_set(self, elements):
        """
        Identify a set from a tuple of elements.
        """
        if elements:
            if isinstance(elements[0], Atom) and elements[0].text == "sets":
                if len(elements) == 9:
                    # 'set', size, active, max, offset, expand, contract,
                    # empty, content
                    d = []
                    for i in elements[8][0]:
                        if i:
                            d.append(i[0])
                    return d
        return None


    def parse_small_tuple(self, data):
        """
        Parse data of a small tuple.
        """
        arity = self.parseChar(data[0])
        elements, data = self._parse_seq(arity, data[1:])
        d = self._identify_dict(elements)
        if d is not None:
            return Dict(d), data
        s = self._identify_set(elements)
        if s is not None:
            return Set(s), data
        return Tuple(elements), data


    def parse_large_tuple(self, data):
        """
        Parse data of a big tuple.
        """
        arity = self.parseInt(data[:4])
        elements, data = self._parse_seq(arity, data[4:])
        d = self._identify_dict(elements)
        if d is not None:
            return Dict(d), data
        s = self._identify_set(elements)
        if s is not None:
            return Set(s), data
        return Tuple(elements), data


    def parse_large_big(self, data):
        """
        Parse a big number.
        """
        n = self.parseInt(data[:4])
        sign = self.parseChar(data[4])
        bignum = 0
        for i in xrange(n - 1, -1, -1):
            d = self.parseChar(data[5 + i])
            bignum = bignum * 256 + d
        if sign:
            bignum *= -1
        return Integer(bignum), data[5 + n:]


    def parse_small_big(self, data):
        """
        Parse a small big number.
        """
        n = self.parseChar(data[0])
        sign = self.parseChar(data[1])
        bignum = 0
        for i in xrange(n - 1, -1, -1):
            d = self.parseChar(data[2 + i])
            bignum = bignum * 256 + d
        if sign:
            bignum *= -1
        return Integer(bignum), data[2 + n:]


    def parse_float(self, data):
        """
        Parse a float number.
        """
        floatData = data[:31]
        try:
            nullIndex = floatData.index(chr(0))
            floatStr = floatData[0:nullIndex]
        except ValueError:
            floatStr = floatData
        floatValue = float(floatStr)
        return Float(floatValue), data[31:]


    def parse_new_float(self, data):
        """
        Parse a IEEE float number.
        """
        return Float(struct.unpack("!d", data[:8])[0]), data[8:]


    def parse_small_integer(self, data):
        """
        Parse a small interger (inferior to 255).
        """
        x = self.parseChar(data[0])
        return Integer(x), data[1:]


    def parse_integer(self, data):
        """
        Parse an integer, on 4 bytes.
        """
        x = struct.unpack("!i", data[:4])[0]
        return Integer(x), data[4:]


    def parse_atom(self, data):
        """
        Parse an atom.
        """
        atomLen = self.parseShort(data[:2])
        atomText = data[2:2 + atomLen]
        return Atom(atomText), data[2 + atomLen:]


    def parse_new_reference(self, data):
        """
        Parse a new reference, creating an L{Reference} object.
        """
        idLen = self.parseShort(data[:2])
        nodeName, data = self.binaryToTerm(data[2:])
        nprim = 4 * idLen
        creation = self._parse_creation(data[0])
        refIds = [self._parse_id(data[1:5])]
        data = data[5:]
        for i in xrange(idLen - 1):
            refId = self.parseInt(data[:4])
            refIds.append(refId)
            data = data[4:]
        return Reference(nodeName, refIds, creation), data


    def parse_reference(self, data):
        """
        Parse a reference, creating an L{Reference} object.
        """
        nodeName, data = self.binaryToTerm(data)
        refId = self._parse_id(data[:4])
        creation = self._parse_creation(data[4])
        return Reference(nodeName, refId, creation), data[5:]


    def parse_port(self, data):
        """
        Parse a port, creating an L{Port} object.
        """
        nodeName, data = self.binaryToTerm(data)
        portId = self._parse_id(data[:4], 28)
        creation = self._parse_creation(data[4])
        return Port(nodeName, portId, creation), data[5:]


    def _parse_id(self, data, maxSignificantBits=18):
        """
        Utility function to parse a node identifier.

        @param data: the data to parse, of length 4.
        @type data: C{str}

        @param maxSignificantBits: bit mask used to limit the size of the id.
        @type maxSignificantBits: C{int}

        @return: an int extracted from C{data}, limited to maxSignificantBits.
        @rtype: C{int}
        """
        nodeId = self.parseInt(data) & ((1 << maxSignificantBits) - 1)
        return nodeId


    def _parse_creation(self, data):
        """
        Utility function to parse a creation field on 3 bits.
        """
        return self.parseChar(data) & 3


    def parse_pid(self, data):
        """
        Parse a pid, creating an L{Pid} object.
        """
        nodeName, data = self.binaryToTerm(data)
        nodeId = self._parse_id(data[:4], 28)
        serial = self.parseInt(data[4:8])
        creation = self._parse_creation(data[8])
        return Pid(nodeName, nodeId, serial, creation), data[9:]


    def parse_binary(self, data):
        """
        Parse binary data.
        """
        length = self.parseInt(data[:4])
        return Binary(data[4:4 + length]), data[4 + length:]


    def parse_fun(self, data):
        """
        Parse a fun, creating a L{Fun} object.
        """
        freeVarsLen = self.parseInt(data[:4])
        pid, data = self.binaryToTerm(data[4:])
        module, data = self.binaryToTerm(data)
        index, data = self.binaryToTerm(data)
        uniq, data = self.binaryToTerm(data)
        freeVars, data = self._parse_seq(freeVarsLen, data)
        return Fun(pid, module, index, uniq, freeVars), data


    def parse_new_fun(self, data):
        """
        Parse a new fun, creating a L{NewFun} object.
        """
        freeVarsLen = self.parseInt(data[:4])
        arity = self.parseChar(data[4])
        uniq = data[5:21]
        index = self.parseInt(data[21:25])
        numFree = self.parseInt(data[25:29])
        module, data = self.binaryToTerm(data[29:])
        oldIndex, data = self.binaryToTerm(data)
        oldUniq, data = self.binaryToTerm(data)
        pid, data = self.binaryToTerm(data)
        freeVars, data = self._parse_seq(freeVarsLen, data)
        return NewFun(pid, module, index, uniq, arity, numFree, oldIndex,
                      oldUniq, freeVars), data


    def parse_new_cache(self, data):
        """
        Parse an Atom, putting in the cache.
        """
        index = self.parseChar(data[0])
        atom, data = self.parse_atom(data[1:])
        return Atom(atom.text, index), data


    def parse_cached_atom(self, data):
        """
        Parse an Atom already in the cache.
        """
        index = self.parseChar(data[0])
        return Atom(None, index), data[1:]


    def parse_export(self, data):
        """
        Parse an L{Export} term.
        """
        module, data = self.binaryToTerm(data)
        function, data = self.binaryToTerm(data)
        arity, data = self.binaryToTerm(data)
        return Export(module, function, arity), data


    def parse_bit_binary(self, data):
        """
        Parse a L{BitBinary} term.
        """
        length = self.parseInt(data[:4])
        bits = self.parseChar(data[4])
        return BitBinary(data[5:length + 5], bits), data[length + 5:]


    def binaryToTerms(self, data):
        """
        Parse terms encoded in binary, separated by the C{MAGIC_VERSION} flag.
        """
        while data:
            if data[0] != chr(self.MAGIC_VERSION):
                raise RemainingDataError(data)
            data = data[1:]
            term, data = self.binaryToTerm(data)
            yield term


    def binaryToTerm(self, data):
        """
        Parse one term encoded in binary.
        """
        dataCode = ord(data[0])
        if dataCode not in self.mapping:
            raise UnhandledCode(dataCode)
        term, data = self.mapping[dataCode](data[1:])
        return term, data



theParser = Parser()

binaryToTerms = theParser.binaryToTerms

