import logging
import time

from tqdm import tqdm

from src.data.config import Config
from src.data.utils import create_state_2_constituency_map, scrape_candidate_tables, config

logging.basicConfig(level=logging.INFO)

# scrape candidates tables as html files...........................................
def get_candidates_tables_for_all_states(year):
    states = list(create_state_2_constituency_map(year).keys())
    for state in tqdm(states):
        logging.info(f'current state : {state}')
        scrape_candidate_tables(state, year)


# scrape individual candidates info tables as html files

if __name__ == '__main__':
    for year in [2004, 2009, 2014]:
        with open(config.missing_constituency, 'a') as handle:
            handle.write(f'Year: {year}..................................................\n')
            get_candidates_tables_for_all_states(year)
            handle.write('==============================================================')