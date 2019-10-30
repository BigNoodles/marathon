#!/usr/bin/env python3

"""
Script to scrape marathon data from arrs.run
"""

#imports
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import time
import csv
from collections import namedtuple
import logging
import multiprocessing as mp


#define constants
BASE_URL = 'https://more.arrs.run/rankings'
CSV_FILE = 'runner_data.csv'


RunnerTup = namedtuple('RunnerTup', [
        'url',
        'given_name',
        'surname',
        'birth_date',
        'citizenship',
        'qualified_by',
        'career_wins',
        'career_prize_money' ])


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




def uniquify(old_list, idfun=None):
    """
    Returns a list of only the unique values of old_list, while preserving order
    """

    if idfun is None:
        def idfun(x): return x

    seen = {}
    new_list = []

    for item in old_list:
        marker = idfun(item)
        if marker in seen: continue
        seen[marker] = 1
        new_list.append(item)

    return new_list



def open_browser(this_url):
    """
    Opens this_url in a Firefox browser or returns None
    """

    logger.info(f'Opening page: {this_url} ...')

    opts = Options()
    opts.headless = True
    browser = Firefox(options=opts)

    try:
        browser.get(this_url)
        return browser
    except Exception as e:
        logger.error(f'Error retrieving {this_url}: {e}')
        return None


def strip_urls(results_table):
    """
    Once we have results on the page, return unique runner URLs
    """
    url_list = []

    t_rows = results_table.find_elements(By.TAG_NAME, "tr")

    for row in t_rows:
        if len(row.find_elements(By.TAG_NAME, 'td')) == 0:
            continue
        icon_cell = row.find_elements(By.TAG_NAME, "td")[8]
        next_url = icon_cell.find_element_by_css_selector('a').get_attribute('href')
        url_list.append(next_url)

    return url_list




def get_to_results(browser):
    """
    From base url, click stuff to get to results page
    """

    logger.info(f'Moving to results page ...')
    time.sleep(5)
    
    dropdown = Select(browser.find_element_by_id('RankingsSearchForm_distance'))
    dropdown.select_by_visible_text('Marathon')

    dropdown = Select(browser.find_element_by_id('RankingsSearchForm_gender'))
    dropdown.select_by_visible_text('Women')

    dropdown = Select(browser.find_element_by_id('RankingsSearchForm_depth'))
    dropdown.select_by_visible_text('Top 50')
    #change to Top 1000 when finished

    find = browser.find_element_by_class_name('btn.btn-primary')
    find.click()

    table_name = 'table.table-striped.table-bordered.table-hover'
    results_table = WebDriverWait(browser, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, table_name))
        )

    women_list = strip_urls(results_table)
    
    #do all that stuff again but for men
    dropdown = Select(browser.find_element_by_id('RankingsSearchForm_gender'))
    dropdown.select_by_visible_text('Men')

    find = browser.find_element_by_class_name('btn.btn-primary')
    find.click()

    WebDriverWait(browser, 60).until(EC.staleness_of(results_table))
    results_table = browser.find_element_by_class_name(table_name)

    men_list = strip_urls(results_table)

    url_list = women_list + men_list
    unique_runners = uniquify(url_list)

    return unique_runners




def scraper_wrapper(url_list):
    """
    Wrapper function for the scraper generator
    """

    logger.info(f'Scraping runners ...')

    runner_records = []

    for next_url in url_list:
        this_runner = scrape_runners(next_url)
        runner_records.append(this_runner)

    return runner_records





def cleanup(browser):
    """
    Closes the webdriver.
    """

    logger.info(f'Closing browser ...')
    browser.close()
    browser.quit()




def scrape_runners(next_url):
    """
    Takes a runner URL and returns a data tuple
    """

    text_css = 'controls.col-sm-9'
    label_css = 'control-label.col-sm-3.label-value'

    browser = open_browser(next_url)

    label_data = browser.find_elements_by_class_name(label_css)
    runner_data = browser.find_elements_by_class_name(text_css)
    labels_text = []
    runners_text = []

    for label in label_data:
        labels_text.append(label.text)

    for runner in runner_data:
        runners_text.append(runner.text)

    runner_dict = dict(zip(labels_text, runners_text))

    given_name = runner_dict['Given name']
    surname = runner_dict['Surname']
    birth_date = runner_dict['Birth date']
    citizenship = runner_dict['Citizenship']
    qualified_by = runner_dict['Qualified by']
    career_prize_money = runner_dict['Career prize money']
    career_wins = runner_dict['Career wins']
    
    next_runner = RunnerTup(next_url, given_name, surname, birth_date, 
            citizenship, qualified_by, career_wins, career_prize_money)

    cleanup(browser)

    yield next_runner





def write_csv(runner_records, this_csv):
    """
    Takes a list of RunnerTup, writes out to csv_file
    """

    logger.info(f'Writing to {this_csv} ...')

    try:
        with open(this_csv, 'w') as writeFile:
            writer = csv.writer(writeFile)
            writer.writerow(list(RunnerTup._fields))
            for runner in runner_records:
                writer.writerow(list(*runner))
            writeFile.close()

    except Exception as e:
        logger.error(f'Error writing to {this_csv}: {e}')




def main():
    this_url = BASE_URL
    this_csv = CSV_FILE

    browser = open_browser(this_url)

    if browser is not None:
        url_list = get_to_results(browser)
        cleanup(browser)
        runner_records = scraper_wrapper(url_list)
        write_csv(runner_records, this_csv)
    else:
        logger.error(f'Failed to open base url.')

    logger.info(f'Done.')


if __name__ == '__main__':
    main()


