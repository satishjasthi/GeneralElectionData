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

# main()

# # add  individual candidate json files to mysql db
candidate_files = glob.glob('/home/satish/Documents/IIMB_Projects/GeneralElectionData/data/external/individual_candidate_tables_json/*.json')

engine = sql.create_engine('mysql+pymysql://root:1234@localhost/ElectionData')

#create table
# engine.execute("CREATE TABLE CandidateFinancialData (C_0 TEXT, C_1 TEXT, C_10 TEXT, C_100 TEXT, C_101 TEXT, C_102 TEXT, C_103 TEXT, C_104 TEXT, C_105 TEXT, C_106 TEXT, C_107 TEXT, C_108 TEXT, C_109 TEXT, C_11 TEXT, C_110 TEXT, C_111 TEXT, C_112 TEXT, C_113 TEXT, C_114 TEXT, C_115 TEXT, C_116 TEXT, C_117 TEXT, C_118 TEXT, C_119 TEXT, C_12 TEXT, C_120 TEXT, C_121 TEXT, C_122 TEXT, C_123 TEXT, C_124 TEXT, C_125 TEXT, C_126 TEXT, C_127 TEXT, C_128 TEXT, C_129 TEXT, C_13 TEXT, C_130 TEXT, C_131 TEXT, C_132 TEXT, C_133 TEXT, C_134 TEXT, C_135 TEXT, C_136 TEXT, C_137 TEXT, C_138 TEXT, C_139 TEXT, C_14 TEXT, C_140 TEXT, C_141 TEXT, C_142 TEXT, C_143 TEXT, C_144 TEXT, C_145 TEXT, C_146 TEXT, C_147 TEXT, C_148 TEXT, C_149 TEXT, C_15 TEXT, C_150 TEXT, C_151 TEXT, C_152 TEXT, C_153 TEXT, C_154 TEXT, C_155 TEXT, C_156 TEXT, C_157 TEXT, C_158 TEXT, C_159 TEXT, C_16 TEXT, C_160 TEXT, C_161 TEXT, C_162 TEXT, C_163 TEXT, C_164 TEXT, C_165 TEXT, C_166 TEXT, C_167 TEXT, C_168 TEXT, C_169 TEXT, C_17 TEXT, C_170 TEXT, C_171 TEXT, C_172 TEXT, C_173 TEXT, C_174 TEXT, C_175 TEXT, C_176 TEXT, C_177 TEXT, C_178 TEXT, C_179 TEXT, C_18 TEXT, C_180 TEXT, C_181 TEXT, C_182 TEXT, C_183 TEXT, C_184 TEXT, C_185 TEXT, C_186 TEXT, C_187 TEXT, C_188 TEXT, C_189 TEXT, C_19 TEXT, C_190 TEXT, C_191 TEXT, C_192 TEXT, C_193 TEXT, C_194 TEXT, C_195 TEXT, C_196 TEXT, C_197 TEXT, C_198 TEXT, C_199 TEXT, C_2 TEXT, C_20 TEXT, C_200 TEXT, C_201 TEXT, C_202 TEXT, C_203 TEXT, C_204 TEXT, C_205 TEXT, C_206 TEXT, C_207 TEXT, C_208 TEXT, C_209 TEXT, C_21 TEXT, C_210 TEXT, C_211 TEXT, C_212 TEXT, C_213 TEXT, C_214 TEXT, C_215 TEXT, C_216 TEXT, C_217 TEXT, C_218 TEXT, C_219 TEXT, C_22 TEXT, C_220 TEXT, C_221 TEXT, C_222 TEXT, C_223 TEXT, C_224 TEXT, C_225 TEXT, C_226 TEXT, C_227 TEXT, C_228 TEXT, C_229 TEXT, C_23 TEXT, C_230 TEXT, C_231 TEXT, C_232 TEXT, C_233 TEXT, C_234 TEXT, C_235 TEXT, C_236 TEXT, C_237 TEXT, C_238 TEXT, C_239 TEXT, C_24 TEXT, C_240 TEXT, C_241 TEXT, C_242 TEXT, C_243 TEXT, C_244 TEXT, C_245 TEXT, C_246 TEXT, C_247 TEXT, C_248 TEXT, C_249 TEXT, C_25 TEXT, C_250 TEXT, C_251 TEXT, C_252 TEXT, C_253 TEXT, C_254 TEXT, C_255 TEXT, C_256 TEXT, C_257 TEXT, C_258 TEXT, C_259 TEXT, C_26 TEXT, C_260 TEXT, C_261 TEXT, C_262 TEXT, C_263 TEXT, C_264 TEXT, C_265 TEXT, C_266 TEXT, C_267 TEXT, C_268 TEXT, C_269 TEXT, C_27 TEXT, C_270 TEXT, C_271 TEXT, C_272 TEXT, C_273 TEXT, C_274 TEXT, C_275 TEXT, C_276 TEXT, C_277 TEXT, C_278 TEXT, C_279 TEXT, C_28 TEXT, C_280 TEXT, C_281 TEXT, C_282 TEXT, C_283 TEXT, C_284 TEXT, C_285 TEXT, C_286 TEXT, C_287 TEXT, C_288 TEXT, C_289 TEXT, C_29 TEXT, C_290 TEXT, C_291 TEXT, C_292 TEXT, C_293 TEXT, C_294 TEXT, C_295 TEXT, C_296 TEXT, C_297 TEXT, C_298 TEXT, C_299 TEXT, C_3 TEXT, C_30 TEXT, C_300 TEXT, C_301 TEXT, C_302 TEXT, C_303 TEXT, C_304 TEXT, C_305 TEXT, C_306 TEXT, C_307 TEXT, C_308 TEXT, C_309 TEXT, C_31 TEXT, C_310 TEXT, C_311 TEXT, C_312 TEXT, C_313 TEXT, C_314 TEXT, C_315 TEXT, C_316 TEXT, C_317 TEXT, C_318 TEXT, C_319 TEXT, C_32 TEXT, C_320 TEXT, C_321 TEXT, C_322 TEXT, C_323 TEXT, C_324 TEXT, C_325 TEXT, C_326 TEXT, C_327 TEXT, C_328 TEXT, C_329 TEXT, C_33 TEXT, C_330 TEXT, C_331 TEXT, C_332 TEXT, C_333 TEXT, C_334 TEXT, C_335 TEXT, C_336 TEXT, C_337 TEXT, C_338 TEXT, C_339 TEXT, C_34 TEXT, C_340 TEXT, C_341 TEXT, C_342 TEXT, C_343 TEXT, C_344 TEXT, C_345 TEXT, C_346 TEXT, C_347 TEXT, C_348 TEXT, C_349 TEXT, C_35 TEXT, C_350 TEXT, C_351 TEXT, C_352 TEXT, C_353 TEXT, C_354 TEXT, C_355 TEXT, C_356 TEXT, C_357 TEXT, C_358 TEXT, C_359 TEXT, C_36 TEXT, C_360 TEXT, C_361 TEXT, C_362 TEXT, C_363 TEXT, C_364 TEXT, C_365 TEXT, C_366 TEXT, C_367 TEXT, C_368 TEXT, C_369 TEXT, C_37 TEXT, C_370 TEXT, C_371 TEXT, C_372 TEXT, C_373 TEXT, C_374 TEXT, C_375 TEXT, C_376 TEXT, C_377 TEXT, C_378 TEXT, C_379 TEXT, C_38 TEXT, C_380 TEXT, C_381 TEXT, C_382 TEXT, C_383 TEXT, C_384 TEXT, C_385 TEXT, C_386 TEXT, C_387 TEXT, C_388 TEXT, C_389 TEXT, C_39 TEXT, C_390 TEXT, C_391 TEXT, C_392 TEXT, C_393 TEXT, C_394 TEXT, C_395 TEXT, C_396 TEXT, C_397 TEXT, C_398 TEXT, C_399 TEXT, C_4 TEXT, C_40 TEXT, C_400 TEXT, C_401 TEXT, C_402 TEXT, C_403 TEXT, C_404 TEXT, C_405 TEXT, C_406 TEXT, C_407 TEXT, C_408 TEXT, C_409 TEXT, C_41 TEXT, C_410 TEXT, C_411 TEXT, C_412 TEXT, C_413 TEXT, C_414 TEXT, C_415 TEXT, C_416 TEXT, C_417 TEXT, C_418 TEXT, C_419 TEXT, C_42 TEXT, C_420 TEXT, C_421 TEXT, C_422 TEXT, C_423 TEXT, C_424 TEXT, C_425 TEXT, C_426 TEXT, C_427 TEXT, C_428 TEXT, C_429 TEXT, C_43 TEXT, C_430 TEXT, C_431 TEXT, C_432 TEXT, C_433 TEXT, C_434 TEXT, C_435 TEXT, C_436 TEXT, C_437 TEXT, C_438 TEXT, C_439 TEXT, C_44 TEXT, C_440 TEXT, C_441 TEXT, C_442 TEXT, C_443 TEXT, C_444 TEXT, C_445 TEXT, C_446 TEXT, C_447 TEXT, C_448 TEXT, C_449 TEXT, C_45 TEXT, C_450 TEXT, C_451 TEXT, C_452 TEXT, C_453 TEXT, C_454 TEXT, C_455 TEXT, C_456 TEXT, C_457 TEXT, C_458 TEXT, C_459 TEXT, C_46 TEXT, C_460 TEXT, C_461 TEXT, C_462 TEXT, C_463 TEXT, C_464 TEXT, C_465 TEXT, C_466 TEXT, C_467 TEXT, C_468 TEXT, C_469 TEXT, C_47 TEXT, C_470 TEXT, C_471 TEXT, C_472 TEXT, C_473 TEXT, C_474 TEXT, C_475 TEXT, C_476 TEXT, C_477 TEXT, C_478 TEXT, C_479 TEXT, C_48 TEXT, C_480 TEXT, C_481 TEXT, C_482 TEXT, C_483 TEXT, C_484 TEXT, C_485 TEXT, C_486 TEXT, C_487 TEXT, C_488 TEXT, C_489 TEXT, C_49 TEXT, C_490 TEXT, C_491 TEXT, C_492 TEXT, C_493 TEXT, C_494 TEXT, C_495 TEXT, C_496 TEXT, C_497 TEXT, C_498 TEXT, C_499 TEXT, C_5 TEXT, C_50 TEXT, C_500 TEXT, C_501 TEXT, C_502 TEXT, C_503 TEXT, C_504 TEXT, C_505 TEXT, C_51 TEXT, C_52 TEXT, C_53 TEXT, C_54 TEXT, C_55 TEXT, C_56 TEXT, C_57 TEXT, C_58 TEXT, C_59 TEXT, C_6 TEXT, C_60 TEXT, C_61 TEXT, C_62 TEXT, C_63 TEXT, C_64 TEXT, C_65 TEXT, C_66 TEXT, C_67 TEXT, C_68 TEXT, C_69 TEXT, C_7 TEXT, C_70 TEXT, C_71 TEXT, C_72 TEXT, C_73 TEXT, C_74 TEXT, C_75 TEXT, C_76 TEXT, C_77 TEXT, C_78 TEXT, C_79 TEXT, C_8 TEXT, C_80 TEXT, C_81 TEXT, C_82 TEXT, C_83 TEXT, C_84 TEXT, C_85 TEXT, C_86 TEXT, C_87 TEXT, C_88 TEXT, C_89 TEXT, C_9 TEXT, C_90 TEXT, C_91 TEXT, C_92 TEXT, C_93 TEXT, C_94 TEXT, C_95 TEXT, C_96 TEXT, C_97 TEXT, C_98 TEXT, C_99 TEXT);")

#create column mapping
with open('/home/satish/Documents/IIMB_Projects/GeneralElectionData/data/interim/IndividualCandidatesFinancialJsonColumns.pckl', 'rb') as handle:
    cand_col_set = list(pickle.load(handle))

candid_col_map = {column:f'C_{indx}' for indx, column in enumerate(cand_col_set)}

with open('/home/satish/Documents/IIMB_Projects/GeneralElectionData/data/interim/CandidColMap.pckl', 'wb') as handle:
    pickle.dump(candid_col_map, handle)

failed_candidates = []
for index, candidate_file in tqdm(enumerate(candidate_files)):
    try:
        with open(candidate_file, 'r') as handle:
            row_dict = json.load(handle)
        new_row = {simp_col:None for simp_col in candid_col_map.values()}
        simple_col_row_dict = {candid_col_map[column]: value for column, value in row_dict.items()}
        new_row.update(simple_col_row_dict)
        df = pd.DataFrame()
        df = df.append(new_row, ignore_index=True)
        df.to_csv('/home/satish/Downloads/deleteme.csv',index=False)
        df.to_sql('CandidateFinancialData', con=engine, if_exists='append',index=False)

    except Exception as e:
        logging.info(e)
        failed_candidates.append(candidate_file)
with open('/home/satish/Documents/IIMB_Projects/GeneralElectionData/data/interim/MissingCandidInDB.txt', 'wb') as handle:
    for candid in failed_candidates:
        handle.write(f'{candid}\n')

