import glob
import json
import pickle
from multiprocessing.dummy import Pool

import pandas as pd
import unicodedata, logging
import os, sys

from tqdm import tqdm
import sqlalchemy as sql

sys.path.append('/home/satish/Documents/IIMB_Projects/GeneralElectionData')
from src.data.config import Config
from src.data.scraper import Scraper

conf = Config()
browser = Scraper(headless=True).launch_browser()


class CandidateInfo:

    def __init__(self, html_file=None):
        self.html_file = html_file
        browser.get(f'file://{self.html_file}')

        # text attribs......................................................................................
        self.name = browser.find_element_by_xpath(conf.candidate.name).text
        self.year = int(browser.find_element_by_xpath(conf.candidate.year).text.split()[-1])
        self.age = browser.find_element_by_xpath(conf.candidate.age).text.split('Age:')[-1]
        self.education = browser.find_element_by_xpath(conf.candidate.education).text
        self.party = browser.find_element_by_xpath(conf.candidate.party).text.split('Party:')[-1]
        self.state = browser.find_element_by_xpath(conf.candidate.state_name).text
        self.constituency = browser.find_element_by_xpath(conf.candidate.constituency).text
        self.num_criminal_cases = \
        browser.find_element_by_xpath(conf.candidate.num_criminal_cases).text.split('No criminal cases')[-1].replace(
            ':', '')
        try:
            self.winning_status = browser.find_element_by_xpath(conf.candidate.winning_status).text
        except:
            self.winning_status = None

        self.candidate_row = {}

        # table attribs........................................................................................
        self.movable_asset_table_raw = browser.find_element_by_xpath(conf.candidate.movable_asset_table)
        self.immovable_asset_table_raw = browser.find_element_by_xpath(conf.candidate.immovable_assest_table)
        self.liabilities_table_raw = browser.find_element_by_xpath(conf.candidate.liabilities)

        self.movable_asset_map = {'Cash': 'MA_T1',
                                  'Deposits in Banks, Financial Institutions and Non-Banking Financial Companies': 'MA_T2',
                                  'Bonds, Debentures and Shares in companies': 'MA_T3',
                                  '(a) NSS, Postal Savings etc': 'MA_T4',
                                  '(b)LIC or other insurance Policies **Not counted in total assets': 'MA_T5',
                                  'Motor Vehicles (details of make, etc.)': 'MA_T6',
                                  'Jewellery (give details weight value)': 'MA_T7',
                                  'Other assets, such as values of claims / interests': 'MA_T8'}

        self.immovable_asset_map = {'Agricultural Land': 'IMA_T1',
                                    'Non Agricultural Land': 'IMA_T2',
                                    'Buildings': 'IMA_T3',
                                    'Houses': 'IMA_T4',
                                    'Others': 'IMA_T5'
                                    }

        self.liabilities_map = {'Loans from Banks': 'L1',
                                'Loans from Financial Institutions': 'L2',
                                '(a) Dues to departments dealing with government accommodation': 'L3',
                                '(b) Dues to departments dealing with supply of water': 'L4',
                                '(c) Dues to departments dealing with supply of electricity': 'L5',
                                '(d) Dues to departments dealing with telephones': 'L6',
                                '(e) Dues to departments dealing with supply of transport': 'L7',
                                '(f) Other Dues if any': 'L8',
                                '(i) (a) Income Tax including surcharge [Also indicate the assessment year upto which Income Tax Return filed.]': 'L9',
                                '(b) Permanent Account Number (PAN)': 'L10',
                                '(ii) Wealth Tax [Also indicate the assessment year upto which Wealth Tax return filed.]': 'L11',
                                '(iii) Sales Tax [Only in case proprietary business]': 'L12',
                                '(iv) Property Tax': 'L13'
                                }

    @staticmethod
    def clean_string(x):
        if type(x)==str:
            return unicodedata.normalize("NFKD", x)
        else:
            return None

    def create_movable_asset_features(self):
        table_flag = False
        html_content = self.movable_asset_table_raw.get_attribute('outerHTML')
        raw_df = pd.read_html(html_content)[0]
        if 'Relation Type' in raw_df or 'Financial Year' in raw_df:
            table_flag = True
            html_content = browser.find_element_by_xpath('/html/body/div[1]/div[2]/a[2]/div/table').get_attribute('outerHTML')
            raw_df = pd.read_html(html_content)[0]
        # set column names
        df = raw_df
        df.columns = raw_df.iloc[0, :]
        df = df.drop(df.index[0])

        row_wise_names = list(df['Description'])
        columnwise_names = list(df.columns)

        if type(columnwise_names[-1]) != str:
            columnwise_names[-1] = 'Row_total'

        descrp_index = columnwise_names.index('Description')
        row_dict = {}
        for i in range(len(row_wise_names)):
            for j in range(descrp_index + 1, len(columnwise_names)):
                value = self.clean_string(df.iloc[i, j])
                key = f"MA_{row_wise_names[i]}_{columnwise_names[j]}"
                row_dict.update({key: value})

        # row_dict = {'MA_T1_self': self.clean_string(df.iloc[1, 2]),
        #             'MA_T1_spouse': self.clean_string(df.iloc[1, 3]),
        #             'MA_T1_dependent1': self.clean_string(df.iloc[1, 4]),
        #             'MA_T1_dependent2': self.clean_string(df.iloc[1, 5]),
        #             'MA_T1_dependent3': self.clean_string(df.iloc[1, 6]),
        #             'MA_T1_total': self.clean_string(df.iloc[1, 7]),
        #
        #             'MA_T2_self': self.clean_string(df.iloc[2, 2]),
        #             'MA_T2_spouse': self.clean_string(df.iloc[2, 3]),
        #             'MA_T2_dependent1': self.clean_string(df.iloc[2, 4]),
        #             'MA_T2_dependent2': self.clean_string(df.iloc[2, 5]),
        #             'MA_T2_dependent3': self.clean_string(df.iloc[2, 6]),
        #             'MA_T2_total': self.clean_string(df.iloc[2, 7]),
        #
        #             'MA_T3_self': self.clean_string(df.iloc[3, 2]),
        #             'MA_T3_spouse': self.clean_string(df.iloc[3, 3]),
        #             'MA_T3_dependent1': self.clean_string(df.iloc[3, 4]),
        #             'MA_T3_dependent2': self.clean_string(df.iloc[3, 5]),
        #             'MA_T3_dependent3': self.clean_string(df.iloc[3, 6]),
        #             'MA_T3_total': self.clean_string(df.iloc[3, 7]),
        #
        #             'MA_T4_self': self.clean_string(df.iloc[4, 2]),
        #             'MA_T4_spouse': self.clean_string(df.iloc[4, 3]),
        #             'MA_T4_dependent1': self.clean_string(df.iloc[4, 4]),
        #             'MA_T4_dependent2': self.clean_string(df.iloc[4, 5]),
        #             'MA_T4_dependent3': self.clean_string(df.iloc[4, 6]),
        #             'MA_T4_total': self.clean_string(df.iloc[4, 7]),
        #
        #             'MA_T5_self': self.clean_string(df.iloc[5, 2]),
        #             'MA_T5_spouse': self.clean_string(df.iloc[5, 3]),
        #             'MA_T5_dependent1': self.clean_string(df.iloc[5, 4]),
        #             'MA_T5_dependent2': self.clean_string(df.iloc[5, 5]),
        #             'MA_T5_dependent3': self.clean_string(df.iloc[5, 6]),
        #             'MA_T5_total': self.clean_string(df.iloc[5, 7]),
        #
        #             'MA_T6_self': self.clean_string(df.iloc[6, 2]),
        #             'MA_T6_spouse': self.clean_string(df.iloc[6, 3]),
        #             'MA_T6_dependent1': self.clean_string(df.iloc[6, 4]),
        #             'MA_T6_dependent2': self.clean_string(df.iloc[6, 5]),
        #             'MA_T6_dependent3': self.clean_string(df.iloc[6, 6]),
        #             'MA_T6_total': self.clean_string(df.iloc[6, 7]),
        #
        #             'MA_T7_self': self.clean_string(df.iloc[7, 2]),
        #             'MA_T7_spouse': self.clean_string(df.iloc[7, 3]),
        #             'MA_T7_dependent1': self.clean_string(df.iloc[7, 4]),
        #             'MA_T7_dependent2': self.clean_string(df.iloc[7, 5]),
        #             'MA_T7_dependent3': self.clean_string(df.iloc[7, 6]),
        #             'MA_T7_total': self.clean_string(df.iloc[7, 7]),
        #
        #             'MA_T8_self': self.clean_string(df.iloc[8, 2]),
        #             'MA_T8_spouse': self.clean_string(df.iloc[8, 3]),
        #             'MA_T8_dependent1': self.clean_string(df.iloc[8, 4]),
        #             'MA_T8_dependent2': self.clean_string(df.iloc[8, 5]),
        #             'MA_T8_dependent3': self.clean_string(df.iloc[8, 6]),
        #             'MA_T8_total': self.clean_string(df.iloc[8, 7]),
        #             }

        return row_dict, table_flag

    def create_immovable_asset_features(self, table_flag):
        html_content = self.immovable_asset_table_raw.get_attribute('outerHTML')
        raw_df = pd.read_html(html_content)[0]
        if table_flag:
            html_content = browser.find_element_by_xpath('/html/body/div[1]/div[2]/a[3]/div/table').get_attribute(
                'outerHTML')
            raw_df = pd.read_html(html_content)[0]
            # row_dict = {'IMA_T1_self': self.clean_string(df.iloc[1, 2]),
            #             'IMA_T1_spouse': self.clean_string(df.iloc[1, 3]),
            #             'IMA_T1_dependent1': self.clean_string(df.iloc[1, 4]),
            #             'IMA_T1_dependent2': self.clean_string(df.iloc[1, 5]),
            #             'IMA_T1_dependent3': self.clean_string(df.iloc[1, 6]),
            #             'IMA_T1_total': self.clean_string(df.iloc[1, 7]),
            #
            #             'IMA_T2_self': self.clean_string(df.iloc[2, 2]),
            #             'IMA_T2_spouse': self.clean_string(df.iloc[2, 3]),
            #             'IMA_T2_dependent1': self.clean_string(df.iloc[2, 4]),
            #             'IMA_T2_dependent2': self.clean_string(df.iloc[2, 5]),
            #             'IMA_T2_dependent3': self.clean_string(df.iloc[2, 6]),
            #             'IMA_T2_total': self.clean_string(df.iloc[2, 7]),
            #
            #             'IMA_T3_self': self.clean_string(df.iloc[3, 2]),
            #             'IMA_T3_spouse': self.clean_string(df.iloc[3, 3]),
            #             'IMA_T3_dependent1': self.clean_string(df.iloc[3, 4]),
            #             'IMA_T3_dependent2': self.clean_string(df.iloc[3, 5]),
            #             'IMA_T3_dependent3': self.clean_string(df.iloc[3, 6]),
            #             'IMA_T3_total': self.clean_string(df.iloc[2, 7]),
            #
            #             'IMA_T4_self': self.clean_string(df.iloc[4, 2]),
            #             'IMA_T4_spouse': self.clean_string(df.iloc[4, 3]),
            #             'IMA_T4_dependent1': self.clean_string(df.iloc[4, 4]),
            #             'IMA_T4_dependent2': self.clean_string(df.iloc[4, 5]),
            #             'IMA_T4_dependent3': self.clean_string(df.iloc[4, 6]),
            #             'IMA_T4_total': self.clean_string(df.iloc[4, 7]),
            #
            #             'IMA_T5_self': self.clean_string(df.iloc[5, 2]),
            #             'IMA_T5_spouse': self.clean_string(df.iloc[5, 3]),
            #             'IMA_T5_dependent1': self.clean_string(df.iloc[5, 4]),
            #             'IMA_T5_dependent2': self.clean_string(df.iloc[5, 5]),
            #             'IMA_T5_dependent3': self.clean_string(df.iloc[5, 6]),
            #             'IMA_T5_total': self.clean_string(df.iloc[5, 7]),
            #             }
            # set column names
        df = raw_df
        df.columns = raw_df.iloc[0, :]
        df = df.drop(df.index[0])

        row_wise_names = list(df['Description'])
        columnwise_names = list(df.columns)

        if type(columnwise_names[-1]) != str:
            columnwise_names[-1] = 'Row_total'

        descrp_index = columnwise_names.index('Description')
        row_dict = {}
        for i in range(len(row_wise_names)):
            for j in range(descrp_index + 1, len(columnwise_names)):
                value = self.clean_string(df.iloc[i, j])
                key = f"IMA_{row_wise_names[i]}_{columnwise_names[j]}"
                row_dict.update({key: value})
        return row_dict, table_flag

    def create_liability_features(self, table_flag):
        html_content = self.liabilities_table_raw.get_attribute('outerHTML')
        raw_df = pd.read_html(html_content)[0]
        if table_flag:
            html_content = browser.find_element_by_xpath('/html/body/div[1]/div[2]/a[4]/div/table').get_attribute(
                'outerHTML')
            raw_df = pd.read_html(html_content)[0]
        # features = df[1]
        # values = df[2]
        # row_update = {}
        # for index, feature in enumerate(features[1:-1]):
        #     row_update.update({f'L{index + 1}': self.clean_string(values[index + 1])})
        df = raw_df
        df.columns = raw_df.iloc[0, :]
        df = df.drop(df.index[0])

        row_wise_names = list(df['Description'])
        columnwise_names = list(df.columns)

        if type(columnwise_names[-1]) != str:
            columnwise_names[-1] = 'Row_total'

        descrp_index = columnwise_names.index('Description')
        row_dict = {}
        for i in range(len(row_wise_names)):
            for j in range(descrp_index + 1, len(columnwise_names)):
                value = self.clean_string(df.iloc[i, j])
                key = f"LB_{row_wise_names[i]}_{columnwise_names[j]}"
                row_dict.update({key: value})
        return row_dict

    def create_candidate_row(self):
        personal_details = {'name': self.name,
                            'age': self.age,
                            'education': self.education,
                            'party': self.party,
                            'state': self.state,
                            'constituency': self.constituency,
                            'num_criminal_cases': self.num_criminal_cases,
                            'winning_status': self.winning_status
                            }
        self.candidate_row.update(personal_details)
        row_dict, table_flag = self.create_movable_asset_features()
        self.candidate_row.update(row_dict)
        row_dict, table_flag = self.create_immovable_asset_features(table_flag)
        self.candidate_row.update(row_dict)
        self.candidate_row.update(self.create_liability_features(table_flag))
        return self.candidate_row

    def close(self):
        browser.close()

def populate_candidate_details():
    rows_list = []
    files = glob.glob('/home/satish/Documents/IIMB_Projects/GeneralElectionData/data/external/individual_candidate_tables/*.html')
    for index, file in tqdm(enumerate(files)):
        # print(file)
        # candid = CandidateInfo(html_file=file)
        # rows_list.append(candid.create_candidate_row())
        try:
            candid = CandidateInfo(html_file=file)
            rows_list.append(candid.create_candidate_row())
        except Exception as e:
            print(e)
            rows_list.append(index)
            with open(
                    '/home/satish/Documents/IIMB_Projects/GeneralElectionData/data/processed/IndividualCandidateFinInfoList.pckl',
                    'wb') as handle:
                pickle.dump(rows_list, handle)
            pass
    candid.close()
    with open('/home/satish/Documents/IIMB_Projects/GeneralElectionData/data/processed/IndividualCandidateFinInfoList.pckl', 'wb') as handle:
        pickle.dump(rows_list, handle)

def save_candid_row_as_json(html_file):
    try:
        candid = CandidateInfo(html_file=html_file)
        row_dict = candid.create_candidate_row()
        json_name = html_file.split('/')[-1].split('.html')[0]
        with open(f'/home/satish/Documents/IIMB_Projects/GeneralElectionData/data/external/individual_candidate_tables_json/{json_name}.json', 'w') as handle:
            json.dump(row_dict, handle)
    except Exception as e:
        print(html_file)
        print(e)
        pass

def main():
    files = glob.glob(
        '/home/satish/Documents/IIMB_Projects/GeneralElectionData/data/external/individual_candidate_tables_old/*.html')
    # pool = Pool(processes=3)
    # ops =  [pool.apply_async(save_candid_row_as_json, args=(html_file,)) for html_file in files]
    # result = [op.get() for op in ops]
    for html_file in tqdm(files):
        save_candid_row_as_json(html_file)

# populate_candidate_details()
# candid = CandidateInfo(html_file='/home/satish/Documents/IIMB_Projects/GeneralElectionData/data/external/individual_candidate_tables/Punjab_Fatehgarh Sahib_Paramjit Singh.html')
# print(candid.create_candidate_row())

main()

# # add  individual candidate json files to mysql db
# candidate_files = glob.glob('/home/satish/Documents/IIMB_Projects/GeneralElectionData/data/external/individual_candidate_tables_json/*.json')
#
# engine = sql.create_engine('mysql+pymysql://root:1234@localhost/ElectionData')
#
# #create column mapping
# with open('/home/satish/Documents/IIMB_Projects/GeneralElectionData/data/external/individual_candidate_tables_json/Andhra Pradesh_Nellore_Syed Hamza Hussainy.json', 'r') as handle:
#     cand_dict = json.load(handle)
#
# column_map = {col:f'C_{indx}' for indx,col in enumerate(cand_dict.keys())}
# failed_candidates = []
# for candidate_file in tqdm(candidate_files):
#     try:
#         with open(candidate_file, 'r') as handle:
#             candidate_row = json.load(handle)
#         new_row = {}
#         for col in candidate_row.keys():
#             new_row.update({column_map[col] : candidate_row[col]})
#         candidate_df = pd.DataFrame()
#         candidate_df = candidate_df.append(new_row, ignore_index=True)
#         candidate_df.to_sql('CandidateFinancialData', con=engine, if_exists='append')
#     except Exception as e:
#         print('Failed')
#         failed_candidates.append(candidate_file)
# print(failed_candidates)
