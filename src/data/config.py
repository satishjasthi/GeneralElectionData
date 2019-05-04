import logging
from string import Template

from src.data import src_data_path

class Candidate:
    pass

class Config:
    def __init__(self):
        self.state_2_constituency_map = Template('{}/state_2_constituency_map_$year'.format(src_data_path.joinpath(src_data_path.parents[2], 'data/raw/')))
        self.general_election_candidate_data = Template('{}/GE_$year.csv'.format(src_data_path.joinpath(src_data_path.parents[2], 'data/raw/')))
        self.constituencies_page_link={2004:'http://www.myneta.info/loksabha2004/',
                                       2009:'http://www.myneta.info/ls2009/',
                                       2014:'http://www.myneta.info/ls2014/'
                                       }
        self.candidates_table_xpath = '//*[@id="table1"]'
        self.candidates_table_savepath = Template('{}'.format(src_data_path.joinpath(src_data_path.parents[2], 'data/external/candidates_tables_html/')) + '/${state}_${constituency}_${year}.html')
        self.missing_constituency_log = src_data_path.joinpath(src_data_path.parents[2], 'data/external/missing_constituencies.log')
        self.missing_constituencies_logger = logging.FileHandler(filename=self.missing_constituency_log)
        self.candidates_tables = src_data_path.joinpath(src_data_path.parents[2], 'data/external/candidates_tables_html/')
        self.individual_candidate_xpath = '/html/body/table/tbody/tr[*]/td[1]/a'
        self.candidate_id_mapper = src_data_path.joinpath(src_data_path.parents[2], 'data/interim/candidate_id_mapper.pckl')
        self.constituency_objects_mapper = src_data_path.joinpath(src_data_path.parents[2], 'data/interim/constituency_objects_mapper.pckl')
        self.individual_candidate_tables_dir = src_data_path.joinpath(src_data_path.parents[2], 'data/external/individual_candidate_tables/')
        self.individual_candidate_tables_page_xpath = '/html/body/div[4]/div'
        self.candidate_detailed_info_table_xpaths = {'brief_info':'/html/body/div[4]/div/div[2]/div[1]/div/div[1]',
                                                     'asset_liability_overview' : '/html/body/div[4]/div/div[2]/div[1]/div/div[6]',
                                                     'education_details': '/html/body/div[4]/div/div[2]/div[1]/div/div[7]/div',
                                                     'criminal_case_table': '/html/body/div[4]/div/div[2]/div[3]/table',
                                                     'Movable_assets_table': '/html/body/div[4]/div/div[2]/a[1]/div/table',
                                                     'Immovable_assets_table': '/html/body/div[4]/div/div[2]/a[2]/div/table',
                                                     'Liabilities_table': '/html/body/div[4]/div/div[2]/a[3]/div/table'
                                                     }

        #individual candidate info table xpaths
        self.candidate = Candidate()
        # const
        self.candidate.name = '/html/body/div[1]/div[2]/div[1]/div/div[1]/h2'
        self.candidate.age = '/html/body/div[1]/div[2]/div[1]/div/div[1]/div[3]'
        self.candidate.year = '/html/body/div[1]/div[2]/h2'
        self.candidate.state_name = '/html/body/div[1]/div[1]/a[3]'
        self.candidate.constituency = '/html/body/div[1]/div[1]/a[4]'
        self.candidate.party = '/html/body/div[1]/div[2]/div[1]/div/div[1]/div[1]'
        self.candidate.education = '/html/body/div[1]/div[2]/div[1]/div/div[7]/div/div[1]'


        # var
        self.candidate.num_criminal_cases = '/html/body/div[1]/div[2]/div[1]/div/div[5]/div/div'
        self.candidate.winning_status = '/html/body/div[1]/div[2]/div[1]/div/div[1]/h2/font'

        # Financial tables
        self.candidate.movable_asset_table = '/html/body/div[1]/div[2]/a[1]/div/table'
        self.candidate.immovable_assest_table = '/html/body/div[1]/div[2]/a[2]/div/table'
        self.candidate.liabilities = '/html/body/div[1]/div[2]/a[3]/div/table'
if __name__ == '__main__':
    o =Config()
