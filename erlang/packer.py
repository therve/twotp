# -*- test-case-name: erlang.test.test_packer-*-
# Copyright (c) 2007-2008 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Build data for an erlang node.
"""

import struct

from erlang.term import ConstantHolder



class UnhandledClass(KeyError):
    """
    Exception raised when trying to pack an uhandled class.
    """



class Packer(ConstantHolder):
    """
    Build binary data from erlang terms.
    """

    MAX_INT = pow(2, 32)
    MAX_SHORT = pow(2, 16)
    MAX_CHAR = pow(2, 8)

    def packChar(self, char):
        """
        Pack an integer between 0 and 255 into a byte.

        @raises ValueError: if the number is too big.
        """
        return chr(char)


    def packShort(self, short):
        """
        Pack a short integer into 2 bytes.

        @raises ValueError: if the number is too big.
        """
        if short >= self.MAX_SHORT:
            raise ValueError("Number too big to fit in short: %s" % (short,))
        return struct.pack("!H", short)


    def packInt(self, integer):
        """
        Pack an integer into 4 bytes.

        @raises ValueError: if the number is too big.
        """
        if integer >= self.MAX_INT:
            raise ValueError("Number too big to fit in int: %s" % (integer,))
        return struct.pack("!I", integer)


    def _pack_id(self, term, maxSignificantBits=18):
        """
        Helper method to build an erlang integer identifier.
        """
        return self.packInt(term & ((1 << maxSignificantBits) - 1))


    def _pack_creation(self, term):
        """
        Helper method to build an integer creation term on 3 bits.
        """
        return self.packChar(term & ((1 << 2) - 1))


    def pack_int(self, term):
        """
        Pack an integer.
        """
        if 0 <= term < self.MAX_CHAR:
            return self.packChar(self.MAGIC_SMALL_INTEGER) + self.packChar(term)
        else:
            return self.packChar(self.MAGIC_INTEGER) + self.packInt(term)


    def pack_float(self, term):
        """
        Pack a float.
        """
        term = "%.20e" % (term,)
        packetData = self.packChar(self.MAGIC_FLOAT)
        packetData += term
        return packetData + "\0" * 5


    def pack_str(self, term):
        """
        Pack a string.
        """
        if len(term) < self.MAX_SHORT:
            return self.packChar(self.MAGIC_STRING) + self.packShort(len(term)) + term
        else:
            return self.pack_list(map(lambda c: ord(c), term))


    def pack_pid(self, term):
        """
        Pack a Pid term.
        """
        node = self.packOneTerm(term.nodeName)
        nodeId = self._pack_id(term.nodeId, 28)
        serial = self.packInt(term.serial)
        creation = self._pack_creation(term.creation)
        return self.packChar(self.MAGIC_PID) + node + nodeId + serial + creation


    def pack_atom(self, term):
        """
        Pack an Atom term.
        """
        return self.packChar(self.MAGIC_ATOM) + self.packShort(len(term.text)) + term.text


    def pack_reference(self, term):
        """
        Pack a Reference term, either classic or new.
        """
        if isinstance(term.refIds, int):
            return self._pack_reference(term)
        else:
            return self._pack_new_reference(term)


    def _pack_reference(self, term):
        """
        Helper method to pack classic Reference.
        """
        node = self.packOneTerm(term.nodeName)
        creation = self._pack_creation(term.creation)
        refId = self._pack_id(term.refIds)
        return self.packChar(self.MAGIC_REFERENCE) + node + refId + creation


    def _pack_new_reference(self, term):
        """
        Helper method to pack new Reference.
        """
        node = self.packOneTerm(term.nodeName)
        creation = self._pack_creation(term.creation)
        ids = self._pack_id(term.refIds[0])
        for i in term.refIds[1:]:
            ids += self.packInt(i)
        return (self.packChar(self.MAGIC_NEW_REFERENCE) +
                self.packShort(len(term.refIds)) + node + creation + ids)


    def pack_tuple(self, term):
        """
        Pack a tuple, either SMALL or LARGE.
        """
        length = len(term)
        if length < self.MAX_CHAR:
            packetData = (self.packChar(self.MAGIC_SMALL_TUPLE) +
                          self.packChar(length))
        else:
            packetData = (self.packChar(self.MAGIC_LARGE_TUPLE) +
                          self.packInt(length))
        for item in term:
            packetData += self.packOneTerm(item)
        return packetData


    def pack_list(self, term):
        """
        Pack a list nil terminated.
        """
        length = len(term)
        if length == 0:
            return self.packChar(self.MAGIC_NIL)
        else:
            packetData = self.packChar(self.MAGIC_LIST) + self.packInt(length)
            for item in term:
                packetData += self.packOneTerm(item)
            return packetData + self.packChar(self.MAGIC_NIL)


    def packOneTerm(self, term):
        """
        Try to pack a term into binary data.
        """
        termType = term.__class__.__name__.lower()
        try:
            meth = getattr(self, 'pack_%s' % (termType,))
        except AttributeError:
            raise UnhandledClass(termType)
        else:
            return meth(term)


    def termToBinary(self, term):
        """
        Build a full binary to be send over the wire.
        """
        return chr(self.MAGIC_VERSION) + self.packOneTerm(term)



thePacker = Packer()

termToBinary = thePacker.termToBinary

