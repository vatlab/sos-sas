
from sos_notebook.test_utils import NotebookTest
import random

class TestDataExchange(NotebookTest):
    def test_get_data_in_lib_from_SAS(self, notebook):
        notebook.call('''
            DATA MYCLASS;
                INPUT NAME $ 1-8 SEX $ 10 AGE 12-13 HEIGHT 15-16 WEIGHT 18-22;
            CARDS;
            JOHN     M 12 59 99.5
            JAMES    M 12 57 83.0
            ALFRED   M 14 69 112.5
            ALICE    F 13 56 84.0
            PROC PRINT;
            RUN;        
        ''')
        assert 'JAMES' in notebook.check_output(f'''\
            %get myclass --from SAS
            myclass
            ''', kernel='SoS')

    def test_get_data_in_lib_from_SAS(self, notebook):
        assert 'DATE' in notebook.check_output(f'''\
            %get sashelp.air --from SAS
            sashelp_air
            ''', kernel='SoS')

    def test_put_to_SAS(self, notebook):
        notebook.call('''\
            import pandas as pd
            df = pd.DataFrame({'Name': ['Henry', 'Linda'],
                'Sex': ['MALE', 'FEMALE']
            })''', kernel='SoS')
        assert 'Henry' in notebook.check_output(f'''\
            %get df
            PROC PRINT;
            RUN;
            ''', kernel='SAS')