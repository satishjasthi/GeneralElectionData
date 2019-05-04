import glob
import logging
import time
from pathlib import Path
import multiprocessing as mp
from multiprocessing.dummy import Pool

import pandas as pd
import pickle

from selenium.webdriver import ActionChains
from tqdm import tqdm

from src.data import src_data_path
from src.data.config import Config
from src.data.scraper import Scraper
from src.data.state2constituency_maps import state2constituency_mapper

config = Config()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)
logger.addHandler(config.missing_constituencies_logger)

class ConstituencyObject:
    pass

def extract_mapping(year):
    file_path = Path(config.state_2_constituency_map.substitute(year=year))
    if not file_path.exists():
        general_election_candidate_data = pd.read_csv(config.general_election_candidate_data.substitute(year=year))
        state_constituency_map={}
        for state in general_election_candidate_data.STATE_NAME.unique():
            state_constituency_map[state] = general_election_candidate_data[general_election_candidate_data.STATE_NAME==state].PC_NAME.unique()
        with open(file_path, 'wb') as handle:
            pickle.dump(state_constituency_map, handle)
    else:
        with open(file_path, 'rb') as handle:
            state_constituency_map = pickle.load(handle)
    return state_constituency_map[year]


def create_state_2_constituency_map(year):
    state_constituency_map = state2constituency_mapper[year]
    return state_constituency_map


def scrape_candidate_tables(state, year):
    constituency_objects = create_constituency_objects_mapper()[year][state]
    # parallel
    # pool = Pool(processes=2)
    # results = [pool.apply_async(get_constituency_candidates_table, args=(constituency_object,)) for constituency_object in constituency_objects]
    # output = [p.get() for p in results]

    # sequential
    for constituency_object in constituency_objects:
        get_constituency_candidates_table(constituency_object, year)

def create_constituency_objects_mapper():
    if not config.constituency_objects_mapper.exists():
        logging.info('Creating constituency_objects_mapper........')
        constituency_objects_mapper = {}
        years = config.constituencies_page_link.keys()
        for year in tqdm(years):
            states = list(create_state_2_constituency_map(year).keys())
            constituency_objects_mapper[year] = {}
            for state in tqdm(states):
                constituency_objects_mapper[year][state] = {}
                constituency_objects = []
                browser = Scraper().launch_browser()
                browser.get(config.constituencies_page_link[year])
                state_2_constituency_map = create_state_2_constituency_map(year)
                constituencies = state_2_constituency_map[state]
                for constituency in constituencies:
                    constituency_object = ConstituencyObject()
                    if year == 2014:
                        constituency_object.name = constituency.strip().upper()
                    else:
                        constituency_object.name = constituency.strip().title()
                    constituency_object.page_url= config.constituencies_page_link[year]
                    constituency_object.candidate_table_xpath=config.candidates_table_xpath
                    constituency_object.candidate_table_savepath=config.candidates_table_savepath.substitute(state=state,
                                                                                                             constituency=constituency,
                                                                                                             year=year
                                                                                                             )
                    constituency_objects.append(constituency_object)
                browser.close()
                constituency_objects_mapper[year][state] = constituency_objects
        with open(config.constituency_objects_mapper, 'wb') as handle:
            pickle.dump(constituency_objects_mapper, handle)
    else:
        with open(config.constituency_objects_mapper, 'rb') as handle:
            # constituency_objects_mapper = pickle.load(handle)
            unpickler = pickle.Unpickler(handle)
            constituency_objects_mapper = unpickler.load()
    return constituency_objects_mapper


def get_constituency_candidates_table(constituency_object, year):
    print(constituency_object.candidate_table_savepath)
    if not Path(constituency_object.candidate_table_savepath).exists():
        logging.info(f'current constituency :{constituency_object.name}')
        browser = Scraper().launch_browser()
        # open homepage
        browser.get(constituency_object.page_url)
        # open given constituency candidates table page
        try:
            # if constituency name matches completely
            if year == 2014:
                constituency_url = browser.find_element_by_link_text(constituency_object.name.upper()).get_attribute('href')
                browser.get(constituency_url)
            else:
                constituency_url = browser.find_element_by_link_text(constituency_object.name).get_attribute('href')
                browser.get(constituency_url)
        except Exception as e:
            logging.info(e)
            try:
                if year==2014:
                    # if constituency name matches partially
                    constituency_url = browser.find_element_by_partial_link_text(constituency_object.name.upper()).get_attribute('href')
                    browser.get(constituency_url)
                else:
                    constituency_url = browser.find_element_by_partial_link_text(
                        constituency_object.name).get_attribute('href')
                    browser.get(constituency_url)
            except Exception as e2:
                logging.info(e2)
                # constituency is missing
                logger.info(f'missing data for constituency {constituency_object.name}\n')
                browser.close()
        try:
            element= browser.find_element_by_xpath(constituency_object.candidate_table_xpath)
            candidates_table_html = element.get_attribute('outerHTML')

            with open(constituency_object.candidate_table_savepath, 'w') as handle:
                handle.write(candidates_table_html)
            browser.close()
        except Exception as e:
            logging.info(e)
            try:
                logging.info(e)
                browser.refresh()
                browser.get(constituency_url)
                time.sleep(5)
                element= browser.find_element_by_xpath(constituency_object.candidate_table_xpath)
                mouse = ActionChains(browser)
                mouse.move_to_element(element)
                mouse.perform()
                candidates_table_html = element.get_attribute('outerHTML')

                with open(constituency_object.candidate_table_savepath, 'w') as handle:
                    handle.write(candidates_table_html)
                browser.close()
            except Exception as e2:
                logging.info(e2)
                logger.info(f'missing data for constituency {constituency_object.name}\n')


    else:
        logging.info('loading from archive')
        logging.info(f'{constituency_object.name} already exists')


def fetch_detailed_candidate_info(year, candidate_id):
    candidate_file = get_candidate_file(year, candidate_id)
    valid_candidate_file = candidate_file.replace('/', ' ')
    if not src_data_path.joinpath(config.individual_candidate_tables_dir,valid_candidate_file).exists():
        url = f'{config.constituencies_page_link[year]}/candidate.php?candidate_id={candidate_id}'
        browser = Scraper().launch_browser()
        browser.get(url)

        try:
            page = browser.find_element_by_xpath(config.individual_candidate_tables_page_xpath)
            page_html = page.get_attribute('outerHTML')
            with open(src_data_path.joinpath(config.individual_candidate_tables_dir,valid_candidate_file), 'w') as handle:
                handle.write(page_html)
        except Exception as e:
            logging.info(f'Unable to find page for candidate: {valid_candidate_file}\n{e}')

        browser.close()

    else:
        return candidate_file

def get_candidate_file(year, candidate_id):
    with open(config.candidate_id_mapper, 'rb') as handle:
        candidate_id_mapper = pickle.load(handle)
    candidate_name = candidate_id_mapper[year][candidate_id]['name']
    state = candidate_id_mapper[year][candidate_id]['state']
    constituency = candidate_id_mapper[year][candidate_id]['constituency']
    candidate_file = f'{state}_{constituency}_{candidate_name}_{year}.html'
    return candidate_file


def create_candidate_info_mapping():
    if not config.candidate_id_mapper.exists():
        logging.info('Creating candidate info mapping for state, constituency\n'
                     'name')
        candidate_id_map = {}
        for year in tqdm([2004, 2009, 2014]):
            candidate_id_map[year] = {}
            logging.info(src_data_path.joinpath(src_data_path.parents[2], 'data/external/candidates_tables_html/').as_posix() + f'/*_{year}.html')
            files = glob.glob(src_data_path.joinpath(src_data_path.parents[2], 'data/external/candidates_tables_html/').as_posix() + f'/*_{year}.html')
            for each_file in tqdm(files):
                state = each_file.split('/')[-1].split('_')[0]
                constituency = each_file.split('/')[-1].split('_')[1]
                browser = Scraper().launch_browser()
                browser.get(f'file://{each_file}')
                candidate_name_elements = browser.find_elements_by_xpath(config.individual_candidate_xpath)
                for candidate in tqdm(candidate_name_elements):
                    candidate_id = candidate.get_attribute('href').split('candidate_id=')[-1]

                    candidate_id_map[year][candidate_id] = {}
                    candidate_name = candidate.text
                    candidate_page = f'{config.constituencies_page_link[year]}/candidate.php?candidate_id={candidate_id}'
                    candidate_id_map[year][candidate_id]['state'] = state
                    candidate_id_map[year][candidate_id]['name'] = candidate_name
                    candidate_id_map[year][candidate_id]['constituency'] = constituency
                    candidate_id_map[year][candidate_id]['page_url'] = candidate_page
        with open(config.candidate_id_mapper, 'wb') as handle:
            pickle.dump(candidate_id_map, handle)
    else:
        with open(config.candidate_id_mapper, 'rb') as handle:
            candidate_id_map = pickle.load(handle)
    return candidate_id_map

create_candidate_info_mapping()