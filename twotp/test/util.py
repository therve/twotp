# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
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
