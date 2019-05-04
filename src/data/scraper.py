from src.data import src_data_path

from selenium import webdriver

class Scraper(object):

    def __init__(self, headless=True):
        self.browser = webdriver
        self.headless=headless

    def launch_browser(self):
        options = self.browser.ChromeOptions()
        options.add_argument('headless')
        if self.headless:
            browser_con = self.browser.Chrome(executable_path=(src_data_path.joinpath(src_data_path.absolute().parents[0],
                                                                      'chromedriver')).as_posix(),
                                              chrome_options=options
                                              )#
        else:
            browser_con = self.browser.Chrome(
                executable_path=(src_data_path.joinpath(src_data_path.absolute().parents[0],
                                                        'chromedriver')).as_posix())  #
        return browser_con

if __name__ == '__main__':
    Scraper().launch_browser()
