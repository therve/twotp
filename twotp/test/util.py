# Copyright (c) 2007-2008 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

"""
Test utilities.
"""

from twisted.trial.unittest import TestCase as TrialTestCase



class TestCase(TrialTestCase):
    """
    Specific TestCase class to add some specific functionalities or backport
    recent additions.
    """
    if getattr(TrialTestCase, 'assertIsInstance', None is None):
        def failUnlessIsInstance(self, instance, classOrTuple):
            """
            Assert that the given instance is of the given class or of one of the
            given classes.

            @param instance: the object to test the type (first argument of the
                C{isinstance} call).
            @type instance: any.
            @param classOrTuple: the class or classes to test against (second
                argument of the C{isinstance} call).
            @type classOrTuple: class, type, or tuple.
            """
            if not isinstance(instance, classOrTuple):
                self.fail("%r is not an instance of %s" % (instance, classOrTuple))

        assertIsInstance = failUnlessIsInstance

