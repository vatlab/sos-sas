#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import os
import tempfile
from sos_notebook.test_utils import NotebookTest


class TestInterface(NotebookTest):

    def test_prompt_color(self, notebook):
        '''test color of input and output prompt'''
        idx = notebook.call(
            '''\
            
            %put "this is SAS"
            ''', kernel="SAS")
        assert [156, 212, 249] == notebook.get_input_backgroundColor(idx)
        assert [156, 212, 249] == notebook.get_output_backgroundColor(idx)

    def test_cd(self, notebook):
        '''Support for change of directory with magic %cd'''
        output1 = notebook.check_output('''
            %let rc = %sysfunc(filename(fr,.));
            %let curdir = %sysfunc(pathname(&fr));
            %let rc = %sysfunc(filename(fr));
            %put CURDIR=&curdir
            ''', kernel="SAS")
        curdir1 = [x for x in output1.splitlines() if 'CURDIR=' in x][-1][7:-1]
        notebook.call('%cd ..', kernel="SoS")
        output2 = notebook.check_output('''
            %let rc = %sysfunc(filename(fr,.));
            %let curdir = %sysfunc(pathname(&fr));
            %let rc = %sysfunc(filename(fr));
            %put CURDIR=&curdir
            ''', kernel="SAS")
        curdir2 = [x for x in output2.splitlines() if 'CURDIR=' in x][-1][7:-1]
        assert len(curdir1) > len(curdir2) and curdir1.startswith(curdir2)

    def test_var_names(self, notebook):
        '''Test get/put variables with strange names'''
        # .a.1 => _a_1 in Python (SoS)
        notebook.call(
            '''\
            %get sashelp.air --from SAS
            ''',
            kernel="SoS")
        assert 'DATE' in notebook.check_output('sashelp_air', kernel='SoS')

    def test_sessioninfo(self, notebook):
        '''test support for %sessioninfo'''
        notebook.call("\n%put 'this is SAS')", kernel="SAS")
        assert 'For  Base  SAS  Software' in notebook.check_output(
            '%sessioninfo', kernel="SAS")
