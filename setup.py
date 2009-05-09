#!/usr/bin/env python
# Copyright (c) 2007-2009 Thomas Herve <therve@free.fr>.
# See LICENSE for details.

from distutils.core import setup


def main():
    setup(
        name="twotp",
        version="0.6",
        description=("Twotp is an implementation of the Erlang node protocol "
                     "written in Python, using the Twisted networking engine"),
        author="Thomas Herve",
        author_email="therve@free.fr",
        license="MIT",
        url="http://launchpad.net/twotp",
        packages=["twotp", "twotp/test", "twisted/plugins"]
    )

if __name__ == "__main__":
    main()
