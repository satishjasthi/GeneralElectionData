import logging
import pickle
import time

from tqdm import tqdm

from src.data.config import Config
from src.data.utils import create_state_2_constituency_map, scrape_candidate_tables, config, \
    fetch_detailed_candidate_info

logger = logging.getLogger(__file__)
# scrape candidates tables as html files...........................................
def get_candidates_tables_for_all_states(year):
    states = list(create_state_2_constituency_map(year).keys())
    for state in tqdm(states):
        logging.info(f'current state : {state}')
        scrape_candidate_tables(state, year)


# scrape individual candidates info tables as html files
def get_individual_candidate_tables():
    with open(config.candidate_id_mapper, 'rb') as handle:
        candidate_id_mapper = pickle.load(handle)
    years = candidate_id_mapper.keys()
    print(years)
    for year in tqdm(years):
        candidate_ids = candidate_id_mapper[year].keys()
        for candidate_id in tqdm(candidate_ids):
            fetch_detailed_candidate_info(year, candidate_id)

if __name__ == '__main__':
    # logger.addHandler(config.missing_constituencies_logger)
    # for year in [2004, 2009, 2014]:
    #     logger.info(f'\nYear: {year}..................................................\n')
    #     get_candidates_tables_for_all_states(year)
    #     logger.info('\n==============================================================\n')
    get_individual_candidate_tables()
