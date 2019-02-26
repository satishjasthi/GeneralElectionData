from string import Template

from src.data import src_data_path

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
        self.missing_constituency = src_data_path.joinpath(src_data_path.parents[2], 'data/external/missing_constituencies.txt')




if __name__ == '__main__':
    o =Config()
