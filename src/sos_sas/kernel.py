#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import os
import shutil
import pandas as pd
import argparse
import tempfile
import saspy
import subprocess
import uuid
from sos.utils import env

# it does not matter what connection method is actually used
# as only borrow code from the SASession to run some "fake"
# submissions. Real submission will be carried out by sos.

# However, because we are using the source code and faking
# some other pieces, we have to lock down to particular versions
# of sospy.
from saspy.sasiostdio import SASsessionSTDIO as SASsession
from saspy.sasiostdio import SASconfigSTDIO

# However, we still need to know the type of connections to
# make some choices
from saspy.sasbase import SASconfig
sas_config = SASconfig()

from io import BytesIO


class sos_SAS(SASsession):
    supported_kernels = {'SAS': ['sas']}
    background_color = '#9CD4F9'
    options = {}
    cd_command = "x 'cd {dir}';"

    def __init__(self, sos_kernel, kernel_name='sas'):
        self.sos_kernel = sos_kernel
        self.kernel_name = kernel_name
        self.init_statements = ''

        #
        # we intentionally do not call SASsessionSTDIO's constructor, which will start SAS
        # We however need to fake stdin, stderr etc in order to work with the session

        #self.pid    = None
        #self.stdin  = None
        #self.stderr = None
        #self.stdout = None

        #self.sascfg   = SASconfigSTDIO(**kwargs)
        #self._log_cnt = 0
        #self._log     = ""
        #self._sb      = kwargs.get('sb', None)

        #self._startsas()
        self.stdin = BytesIO()
        self.stderr = BytesIO()

        self.pid = None
        # mimick a sascfg object with argparse Namespace
        #self.sascfg = argparse.Namespace()

        self._sb = argparse.Namespace()
        self._sb.sascfg = sas_config
        self._sb._dsopts = lambda x: ''
        self._sb.file_info = lambda remotefile, quiet: True
        self._sb.HTML_Style = 'Default'

        self._log_cnt = 0
        self._log = ""

        self.sascfg = SASconfigSTDIO(self)
        # self.sascfg.encoding = 'utf-8'
        # self.sascfg.output = 'text'
        # self.sascfg.verbose = False
        # self.sascfg.tunnel = None

        # self._sb = argparse.Namespace()
        # self._sb._dsopts = lambda x: ''
        # self._sb.file_info = lambda remotefile, quiet: True
        # self._sb.HTML_Style = 'Default'

    def submit(self, code, results='html', prompt=None):
        logn = self._logcnt()
        logcodei = f"%put %upcase(e3969440a681a24088859985{logn});"
        logcodeo = f"E3969440A681A24088859985{logn}"

        mj = ";*\';*\";*/;" + logcodei
        # get code to be executed from stdin, and add the submit code.
        code = self.stdin.getvalue().decode('utf-8') + code
        # clear stdin
        self.stdin.seek(0)
        self.stdin.truncate()

        sas_code = mj + '\n' + code + '\n' + mj

        env.log_to_file('Executing in SAS Kernel\n{}'.format(sas_code))

        response = self.sos_kernel.get_response(
            sas_code, ['stream', 'execute_result'],
            name=('stdout', 'stderr', 'data'))
        # response = self.sos_kernel.run_cell(
        #     sas_code,
        #     True,
        #     False,
        #     on_error=f"Failed to execute SAS code {sas_code}")
        res = [
            x[1]['data']['text/html']
            for x in response
            if x[0] == 'execute_result'
        ]
        if res:
            res = res[0]
        else:
            res = ''
        return {'LOG': res, 'LST': res}

    def get_vars(self, names):
        #
        # get variables with names from env.sos_dict and create
        # them in the subkernel. The current kernel should be SAS
        for name in names:
            if not isinstance(env.sos_dict[name], pd.DataFrame):
                if self.sos_kernel._debug_mode:
                    self.sos_kernel.warn(
                        'Cannot transfer a non DataFrame object {} of type {} to SAS'
                        .format(name, env.sos_dict[name].__class__.__name__))
                continue
            # sas cannot handle columns with non-string header
            data = env.sos_dict[name].rename(
                columns={x: str(x) for x in env.sos_dict[name].columns})
            # convert dataframe to SAS, this function will call self.submit,
            # which we have changed to submit to the underlying SAS kernel
            self.dataframe2sasdata(data, name, "")

    def scp_tmp_file(self, filename, temp_name):
        pgm = self.sascfg.ssh.replace('ssh', 'scp')
        if not os.path.isfile(pgm):
            return RuntimeError(f'{pgm} command not found')
        #
        dest_file = temp_name + '_' + os.path.basename(filename)
        params = [pgm]
        if self.sascfg.identity:
            params += ["-i", self.sascfg.identity]

        if self.sascfg.port:
            params += ["-P", self.sascfg.port]

        if self.sascfg.identity:
            params += ["-i", self.sascfg.identity]

        params += [self.sascfg.host + ':' + filename, dest_file]
        env.log_to_file('KERNEL', f'COMMAND "{pgm}" "{params}"')

        subprocess.call(params)
        if not os.path.isfile(dest_file):
            raise RuntimeError(
                f'Failed to retrieve {filename} from {self.sascfg.host} with command {" ".join(params)}'
            )
        return dest_file

    def put_vars(self, items, to_kernel=None):
        # create a temporary directory
        temp_name = uuid.uuid4().hex[:8]
        res = {}
        for idx, item in enumerate(items):
            try:
                # the directory will be created if it does not exist
                self.stdin.write(f'''\
%put {temp_name}=%sysfunc(getoption(work));
'''.encode('utf-8'))
                # run the code to save file
                resp = self.submit('')
                # let us get the path to the library
                env.log_to_file('KERNEL', resp['LST'])
                path_name = resp['LST'].split(temp_name + '=')[-1].split('<')[0]

                # self.sos_kernel.run_cell(
                #     code,
                #     True,
                #     False,
                #     on_error="Failed to get data set {} from SAS".format(
                #         item))
                # #
                # check if file exists
                saved_file = os.path.join(path_name, f'{item.lower()}.sas7bdat')
                if os.path.isfile(saved_file):
                    # good, local file or shared file system
                    # now try to read it with Python
                    df = pd.read_sas(saved_file, encoding='utf-8')
                    res[item] = df
                elif sas_config.mode == 'SSH':
                    # SSH, let us copy the file.
                    saved_file = self.scp_tmp_file(saved_file, temp_name)
                    df = pd.read_sas(saved_file, encoding='utf-8')
                    res[item] = df
                    os.remove(saved_file)
                else:
                    self.sos_kernel.warn(
                        '''Failed to access saved file. This version of sos-sas only
                        support retrieving datasets from SAS server that shares the
                        same file system as the Jupyter server, or with SSH connection.'''
                    )
                    continue
            except Exception as e:
                self.sos_kernel.warn(
                    'Failed to get dataset {} from SAS: {}'.format(item, e))

        return res

    def parse_response(self, html):
        # separate response into LOG (with class err) and LST (with class s)
        LOG = ''
        LST = ''
        for line in html.split('</span>'):
            if 'class="err"' in line:
                LOG += line.replace('<br>', '\n').replace(
                    '<span class="err">', '') + ' '
            elif 'class="s"' in line:
                LST += line.replace('<br>', '\n').replace(
                    '<span class="s">', '') + ' '
        return {'LOG': LOG, 'LST': LST}

    def sessioninfo(self):
        # return information of the kernel
        sas_code = '''\
PROC PRODUCT_STATUS;
run;
'''
        response = self.sos_kernel.get_response(
            sas_code, ('execute_result',))[0][1]['data']['text/html']
        return self.parse_response(response)['LOG']
