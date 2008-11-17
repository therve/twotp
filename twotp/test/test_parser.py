# Copyright (c) 2007-2008 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Parser tests.
"""

from twotp.term import Atom, Tuple, Pid, Reference, Integer, String, List
from twotp.term import Float, Port, Binary, Fun, NewFun, Export, BitBinary
from twotp.parser import Parser, RemainingDataError, UnhandledCode
from twotp.test.util import TestCase



class ParseTestCase(TestCase):
    """
    Test parsing various data.
    """

    def setUp(self):
        """
        Create a parser instance.
        """
        self.parser = Parser()


    def test_parseAtom(self):
        """
        Try to parse the binary representation of an atom.
        """
        self.assertEquals(
            self.parser.binaryToTerm("d\x00\x03foo"), (Atom("foo"), ""))


    def test_parseString(self):
        """
        Try to parse the binary representation of a short string object.
        """
        self.assertEquals(
            self.parser.binaryToTerm("k\x00\x04dang"),
            ([100, 97, 110, 103], ""))


    def test_parseNil(self):
        """
        Try to parse NIL value.
        """
        self.assertEquals(self.parser.binaryToTerm("j"), (List([]), ""))


    def test_parseList(self):
        """
        Try to parse a list of integers.
        """
        self.assertEquals(
            self.parser.binaryToTerm("l\x00\x00\x00\x02a\x01a\x02j"),
            (List([1, 2]), ""))


    def test_parseSmallTuple(self):
        """
        Test parsing a small tuple of integer values.
        """
        self.assertEquals(
            self.parser.binaryToTerm("h\x02a\x05a\x04"), (Tuple([5, 4]), ""))


    def test_parseLargeTuple(self):
        """
        Try parsing a large tuple of integers.
        """
        self.assertEquals(
            self.parser.binaryToTerm("i\x00\x00\x00\x02a\x05a\x04"),
            (Tuple([5, 4]), ""))


    def test_parseLargeBig(self):
        """
        Try parsing a positive and negative big integer. The only difference
        between the two binary values is the sign bit.
        """
        self.assertEquals(
            self.parser.binaryToTerm("o\x00\x00\x00\x04\x00\x01\x02\x03\x04"),
            (Integer(67305985), ""))
        self.assertEquals(
            self.parser.binaryToTerm("o\x00\x00\x00\x04\x01\x01\x02\x03\x04"),
            (Integer(-67305985), ""))


    def test_parseSmallBig(self):
        """
        Try parsing a positive and negative small big integer. The only
        difference between the two binary values is the sign bit.
        """
        self.assertEquals(self.parser.binaryToTerm(
            "n\x04\x00\x01\x02\x03\x04"), (Integer(67305985), ""))
        self.assertEquals(self.parser.binaryToTerm(
            "n\x04\x01\x01\x02\x03\x04"), (Integer(-67305985), ""))


    def test_parseFloat(self):
        """
        Test parsing a float null terminated.
        """
        self.assertEquals(
            self.parser.binaryToTerm("c\x31\x32\x2e\x33\x34\x00\x00\x00\x00"
                "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                "\x00\x00\x00\x00\x00\x00\x00"),
            (Float(12.34), ""))


    def test_parseBigFloat(self):
        """
        Try to parse a float without null character.
        """
        self.assertEquals(
            self.parser.binaryToTerm("c\x31\x32\x2e\x33\x34\x32\x32\x32\x32"
                "\x32\x32\x32\x32\x32\x32\x32\x32\x32\x32\x32\x32\x32\x32\x32"
                "\x32\x32\x32\x32\x32\x32\x32"),
            (Float(12.34222222222222222222222222222), ""))
        self.assertEquals(
            self.parser.binaryToTerm("c-5.6779999999999999360"
                "5e+00\x00\x00\x00\x00"),
            (Float(-5.678), ""))


    def test_parseInteger(self):
        """
        Test parsing a standard integer on 32 bits.
        """
        self.assertEquals(
            self.parser.binaryToTerm("b\x00\x00\x00\x0f"), (Integer(15), ""))
        self.assertEquals(
            self.parser.binaryToTerm("b\xff\xff\xff\xff"), (Integer(-1), ""))
        self.assertEquals(
            self.parser.binaryToTerm("b\xff\xff\xff\xfe"), (Integer(-2), ""))


    def test_parseSmallInteger(self):
        """
        Try to parse a small integer on 1 byte.
        """
        self.assertEquals(self.parser.binaryToTerm("a\x0e"), (Integer(14), ""))


    def test_parseNewReference(self):
        """
        Parse a new reference binary representation: the reference ID is an
        array of integers.
        """
        r = Reference(Atom('bar'), [3, 4], 1)
        self.assertEquals(
            self.parser.binaryToTerm("r\x00\x02d\x00\x03bar"
                                     "\x01\x00\x00\x00\x03\x00\x00\x00\x04"),
            (r, ""))


    def test_parseReference(self):
        """
        Parse a reference binary representation: the reference ID is only an
        integer.
        """
        r = Reference(Atom('foo'), 5, 1)
        self.assertEquals(
            self.parser.binaryToTerm("ed\x00\x03foo"
                                     "\x00\x00\x00\x05\x01"),
            (r, ""))


    def test_parsePort(self):
        """
        Parse a Port binary representation.
        """
        r = Port(Atom('egg'), 12, 0)
        self.assertEquals(
            self.parser.binaryToTerm("fd\x00\x03egg\x00\x00\x00\x0c\x04"),
            (r, ""))


    def test_parseBinary(self):
        """
        Parse a binary object representation.
        """
        self.assertEquals(
            self.parser.binaryToTerm("m\x00\x00\x00\x03egg"),
            (Binary("egg"), ""))


    def test_parseFun(self):
        """
        Try to parse a Fun object.
        """
        f = Fun(Pid(Atom('foo'), 1234, 56, 2), Atom("spam"), 12, 34,
                [Atom("bar"), Atom("bim")])
        self.assertEquals(
            self.parser.binaryToTerm("u\x00\x00\x00\x02gd\x00\x03foo"
                "\x00\x00\x04\xd2\x00\x00\x008\x02d\x00\x04spama\x0ca\x22"
                "d\x00\x03bard\x00\x03bim"),
            (f, ""))


    def test_parseNewFun(self):
        """
        Try to parse a NewFun object: it has specific ID fields.
        """
        f = NewFun(Pid(Atom('foo'), 1234, 56, 2), Atom("spam"), 1,
                   '1234567890123456', 1, 2, 12, 34,
                   [Atom("bar"), Atom("bim")])
        self.assertEquals(
            self.parser.binaryToTerm("p\x00\x00\x00\x02\x01"
            "1234567890123456\x00\x00\x00\x01\x00\x00\x00\x02d\x00\x04spam"
            "a\x0ca\x22gd\x00\x03foo\x00\x00\x04\xd2\x00\x00\x008\x02"
            "d\x00\x03bard\x00\x03bim"),
            (f, ""))


    def test_parseNewCache(self):
        """
        Try to parse a NewCache object: it should correctly extract it and
        put it in the cache.
        """
        # This doesn't put it in the cache
        a = Atom("spam")
        self.assertEquals(
            self.parser.binaryToTerm("N\x03\x00\x04spam"), (a, ""))
        # Retrieve the value from cache
        b = Atom(None, 3)
        self.assertEquals(a, b)


    def test_parseCachedAtom(self):
        """
        Try to parse a Cached object: if not in the cache, this should raise
        an exception, and if found it should retrieve it.
        """
        self.assertRaises(KeyError, self.parser.binaryToTerm, "C\x08")
        a = Atom("foo", 8)
        self.assertEquals(self.parser.binaryToTerm("C\x08"), (a, ""))


    def test_unhandledCode(self):
        """
        Check that trying to parse invalid data raises an exception.
        """
        self.assertRaises(UnhandledCode, self.parser.binaryToTerm, "Dfoo")


    def test_parseVersion(self):
        """
        Version shouldn't be managed by the parser
        """
        self.assertRaises(RuntimeError, self.parser.binaryToTerm, "\x83foo")


    def test_parseExport(self):
        """
        Test parsing an export term.
        """
        e = Export(Atom("bar"), Atom("foo"), Integer(2))
        self.assertEquals(
            self.parser.binaryToTerm("qd\x00\x03bard\x00\x03fooa\x02"),
            (e, ""))


    def test_parseBitBinary(self):
        """
        Test parsing a bit binary object.
        """
        b = BitBinary("\x04\x04\x04", 19)
        self.assertEquals(
            self.parser.binaryToTerm("M\x00\x00\x00\x03\x13\x04\x04\x04"),
            (b, ""))


    def test_parseDict(self):
        """
        Test parsing a dict object.
        """
        data = (
            "\x83h\td\x00\x04dicta\x02a\x10a\x10a\x08aPa0h\x10jjjjjjjjjjjjjjjjh"
            "\x01h\x10l\x00\x00\x00\x01l\x00\x00\x00\x02d\x00\x04spama\x01j"
            "jl\x00\x00\x00\x01l\x00\x00\x00\x02d\x00\x03fook\x00\x03bar"
            "jjjjjjjjjjjjjjjj")
        self.assertEquals(
            list(self.parser.binaryToTerms(data)),
            [{Atom("foo"): [98, 97, 114], Atom("spam"): 1}])


    def test_parseSet(self):
        """
        Test parsing a set object.
        """
        data = (
            "\x83h\td\x00\x04setsa\x02a\x10a\x10a\x08aPa0h\x10jjjjjjjjjjjjjjjj"
            "h\x01h\x10l\x00\x00\x00\x01d\x00\x03barjl\x00\x00\x00\x01d\x00"
            "\x03foojjjjjjjjjjjjjjj")
        self.assertEquals(
            list(self.parser.binaryToTerms(data)),
            [set([Atom("bar"), Atom("foo")])])


    def test_binaryToTerms(self):
        """
        Try to parse a full binary stream.
        """
        self.assertEquals(
            list(self.parser.binaryToTerms("\x83d\x00\x03foo")),
            [Atom("foo")])


    def test_remainingData(self):
        """
        If too much data is given, it should raise a C{RemainingDataError}
        exception.
        """
        self.assertRaises(RemainingDataError, list,
            self.parser.binaryToTerms("\x83d\x00\x03foo\x01"))


    def test_compressedData(self):
        """
        The parser is able to handle compressed data.
        """
        self.assertEquals(
            self.parser.binaryToTerm("P\x00\x00\x00\x12x\x9c\xcbf\xe0\xaf@"
                                     "\x05\x00@\xc8\x07\x83"),
            ([120] * 15, ""))


    def test_parseNewFloat(self):
        """
        Try to parse a new float.
        """
        self.assertEquals(self.parser.binaryToTerm('F?\xf3\xae\x14z\xe1G\xae'),
            (1.23, ""))
