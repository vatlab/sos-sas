#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import os
import unittest
from ipykernel.tests.utils import execute, wait_for_idle
from sos_notebook.test_utils import sos_kernel

class TestSASKernel(unittest.TestCase):
    def testSoSKernel(self):
        # Python -> R
        with sos_kernel() as kc:
            iopub = kc.iopub_channel
            # create a data frame
            execute(kc=kc, code='''
%use
''')
            wait_for_idle(kc)


if __name__ == '__main__':
    unittest.main()

