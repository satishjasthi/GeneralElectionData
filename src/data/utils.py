import logging
import time
from pathlib import Path
import multiprocessing as mp
from multiprocessing.dummy import Pool

import pandas as pd
import pickle

from selenium.webdriver import ActionChains

from src.data.config import Config
from src.data.scraper import Scraper

config = Config()
logging.basicConfig(level=logging.INFO)

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
    return state_constituency_map


def create_state_2_constituency_map(year):
    state_constituency_map = extract_mapping(year)
    return state_constituency_map


def scrape_candidate_tables(state, year):
    constituency_objects = create_constituency_objects(state, year)
    pool = Pool(processes=2)
    results = [pool.apply_async(get_constituency_candidates_table, args=(constituency_object,)) for constituency_object in constituency_objects]
    output = [p.get() for p in results]


def create_constituency_objects(state, year):
    constituency_objects = []
    browser = Scraper().launch_browser()
    browser.get(config.constituencies_page_link[year])
    state_2_constituency_map = create_state_2_constituency_map(year)
    constituencies = state_2_constituency_map[state]
    for constituency in constituencies:
        constituency_object = ConstituencyObject()
        constituency_object.name = constituency.strip().title()
        constituency_object.page_url= config.constituencies_page_link[year]
        constituency_object.candidate_table_xpath=config.candidates_table_xpath
        constituency_object.candidate_table_savepath=config.candidates_table_savepath.substitute(state=state,
                                                                                                 constituency=constituency,
                                                                                                 year=year
                                                                                                 )
        constituency_objects.append(constituency_object)
    browser.close()
    return constituency_objects

def get_constituency_candidates_table(constituency_object):
    if not Path(constituency_object.candidate_table_savepath).exists():
        logging.info(f'current constituency :{constituency_object.name}')
        browser = Scraper().launch_browser()
        # open homepage
        browser.get(constituency_object.page_url)
        # open given constituency candidates table page
        constituency_url = browser.find_element_by_link_text(constituency_object.name).get_attribute('href')
        browser.get(constituency_url)
        try:
            element= browser.find_element_by_xpath(constituency_object.candidate_table_xpath)
        except Exception as e:
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
            except Exception as e:
                with open(config.missing_constituency, 'a') as handle:
                    handle.write(f'missing data for constituency {constituency_object.name}\n')

    else:
        logging.info(f'{constituency_object.name} already exists')

