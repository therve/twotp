# -*- test-case-name: twotp.test.test_packer-*-
# Copyright (c) 2007-2008 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Build data for an erlang node.
"""

import struct, zlib

from twotp.term import ConstantHolder, Atom



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
        return struct.pack("!i", integer)


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
        elif -2**31 <= term < 2**31:
            return self.packChar(self.MAGIC_INTEGER) + self.packInt(term)
        else:
            sign = int(term < 0)
            term = abs(term)
            a = 1
            n = 0
            while a < term:
                n += 1
                a = 256 ** n
            if n < 256:
                data = self.packChar(self.MAGIC_SMALL_BIG) + self.packChar(n)
            else:
                data = self.packChar(self.MAGIC_LARGE_BIG) + self.packInt(n)
            data += self.packChar(sign)
            content = []
            for i in xrange(n - 1, -1, -1):
                c = term // 256 ** i
                content.append(self.packChar(c))
                term = term - (256 ** i) * c
            content.reverse()
            return data + ''.join(content)

    pack_long = pack_int


    def pack_float(self, term):
        """
        Pack a float.
        """
        term = "%.20e" % (term,)
        packetData = self.packChar(self.MAGIC_FLOAT)
        packetData += term
        nullPadStr = "\0" * (31 - len(term))
        return packetData + nullPadStr


    def pack_newfloat(self, term):
        """
        Pack a new float.
        """
        return struct.pack("!d", term.value)


    def pack_str(self, term):
        """
        Pack a string.
        """
        if len(term) < self.MAX_SHORT:
            return "%s%s%s" % (self.packChar(self.MAGIC_STRING),
                               self.packShort(len(term)), term)
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


    def pack_dict(self, term):
        """
        Pack a dict.

        Warning: this is not yet complete. It's based on a basic understanding
        of the stdlib dict module in Erlang, but doesn't exactly reproduce the
        same structure. Beware if you use this for now.
        """
        size = len(term)
        slot = (divmod(size, 16)[0] + 1) * 16
        expand = slot * 5
        contract = slot * 3
        empty = ([],) * slot
        emptyRemain = ([],) * (slot - size)
        # XXX: this is mainly where the problem resides: in erlang dict, the
        # keys in the content are placed in a particular order, specified by
        # the phash function. For now, it just put the keys in Python order.
        content = tuple([[list(i)] for i in term.items()])
        d = (Atom('dict'), size, slot, slot, slot/2,
            expand, contract, empty, (content + emptyRemain,))
        return self.packOneTerm(d)


    def pack_set(self, term):
        """
        Pack a set.

        Warning: this is not yet complete. It's based on a basic understanding
        of the stdlib set module in Erlang, but doesn't exactly reproduce the
        same structure. Beware if you use this for now.
        """
        size = len(term)
        slot = (divmod(size, 16)[0] + 1) * 16
        expand = slot * 5
        contract = slot * 3
        empty = ([],) * slot
        emptyRemain = ([],) * (slot - size)
        # XXX: this is mainly where the problem resides: in erlang set, the
        # values in the content are placed in a particular order, specified by
        # the phash function. For now, it just put the values in Python order.
        content = tuple([[i] for i in term])
        d = (Atom('sets'), size, slot, slot, slot/2,
            expand, contract, empty, (content + emptyRemain,))
        return self.packOneTerm(d)


    def _check_string(self, term):
        for i in term:
            if i > 256:
                return False
        return True


    def pack_list(self, term):
        """
        Pack a list nil terminated.
        """
        length = len(term)
        if length == 0:
            return self.packChar(self.MAGIC_NIL)
        else:
            if self._check_string(term) and len(term) < self.MAX_SHORT:
                term = ''.join([chr(i) for i in term])
                return self.packChar(self.MAGIC_STRING) + self.packShort(len(term)) + term

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


    def termToBinary(self, term, compress=0):
        """
        Build a full binary to be send over the wire.
        """
        data = self.packOneTerm(term)
        size = len(data)
        if compress and size >= 32:
            data = "%s%s%s" % (chr(self.MAGIC_COMPRESSED), self.packInt(size),
                               zlib.compress(data, compress))
        return "%s%s" % (chr(self.MAGIC_VERSION), data)



thePacker = Packer()

termToBinary = thePacker.termToBinary

