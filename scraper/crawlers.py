import requests
from bs4 import BeautifulSoup
import json
from parsers import IndeedJob, GeneralParser
from settings import *
from uploaders import PostgresJob
# from selenium import webdriver
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import TimeoutException
# from selenium.webdriver.common.by import By
import time
import random
from uploaders import postgres_database
import sys

class GeneralCrawler(object):
    """General crawler class, assumed paginsed website searched by keyword"""
    def __init__(self, site_name, url, keyword, logger):
        self.current_url = url
        self.site_name = site_name
        self.keyword = keyword
        self.current_html = None
        self.db = PostgresJob(self, logger)
        self.job_type = GeneralParser
        self.checked = 0
        self.posted = 0
        self.time_start = time.time()
        self.logger = logger
        cur = postgres_database.cursor()
        self.logger.info('Downloading current job list')
        cur.execute("SELECT remote_identifier FROM jobs_full ")
        self.logger.info('Creating set of job list')
        self.jobs_scraped = set(x[0] for x in cur.fetchall())
        print len(self.jobs_scraped)

    def __iter__(self):
        yield 'job_url', 'job_id'

    def post_jobs(self):
        session = requests.session()
        for job in self:
            self.checked += 1
            # if not self.db.check_id_existence(job[1]):
            try:
                job_object = self.job_type(job[0], session, remote_identifier=job[1])
                self.db.post_job(job_object)
                self.posted += 1
            except Exception as e:
                self.logger.error("Error parsing {}, {}".format(job[1], str(e)))

    def complete(self):
        self.logger.info("Crawling complete, {} jobs parsed, {} jobs posted".format(self.checked, self.posted))
        self.logger.info("Took {}s".format(time.time() - self.time_start))


class StaticCrawler(GeneralCrawler):
    """General crawler class, assumed paginsed website searched by keyword"""
    def __init__(self, site_name, url, keyword, logger):
        GeneralCrawler.__init__(self, site_name, url, keyword, logger=logger)
        self.s = requests.Session()
        self.get_page()

    def get_page(self):
        x = self.s.get(self.current_url, headers=HEADERS)
        self.current_html = x.content

    def check_loaded(self):
        return True
#
#
# class DynamicCrawler(GeneralCrawler):
#     """General crawler class, assumed paginsed website searched by keyword"""
#     def __init__(self, site_name, url, keyword):
#         GeneralCrawler.__init__(self, site_name, url, keyword)
#         self.d = webdriver.Chrome()
#         self.get_page()
#
#     def get_page(self):
#         self.d.get(self.current_url)
#         self.current_html = self.d.execute_script("return document.getElementsByTagName('html')[0].innerHTML")
# #
#
# class LinkedInCrawler(DynamicCrawler):
#     def __init__(self, keyword):
#         url = next(self.next_query(keyword))
#         self.job_type = LinkedInJob
#         DynamicCrawler.__init__(self, site_name='LinkedIn', url=url, keyword=keyword)
#
#     def get_page(self):
#         delay = 10
#         self.d.get(self.current_url)
#
#         try:
#             WebDriverWait(self.d, delay).until(EC.presence_of_element_located((By.ID, "content-outlet")))
#             self.current_html = self.d.page_source
#         except TimeoutException:
#             print("Loading took too much time!")
#
#     def next_query(self, keyword):
#         cities = {
#             "new york": "2E7-3-0-17-29",
#             "seattle": "2E7-3-0-17-29",
#             "san francisco": "2E7-1-0-38-1",
#             "chicago": "2E1-1-0-16-13",
#             "atlanta": "2E8-4-0-60-2",
#             "los angeles": "2E7-1-0-19-57",
#             "houston": "2E10-4-0-101-12",
#             "dallas": "2E10-4-0-57-5",
#             "washington": "2E8-2-0-1-2"
#         }
#
#         for city, val in cities.items():
#             search_url = 'https://www.linkedin.com/jobs/search?keywords={}&locationId=us%3A0&f_GC=us%{}'.format(keyword, val)
#             print("querying {}".format(city))
#             yield search_url
#
#     def check_loaded(self):
#         soup = BeautifulSoup(self.current_html, 'lxml')
#         trys = 0
#         while trys < 30:
#             try:
#                 _ = soup.find('code', {'id': 'decoratedJobPostingsModule'}).string
#                 return soup
#             except AttributeError as a:
#                 self.get_page()
#                 with open('output.txt', 'w') as out:
#                     out.write(self.current_html.encode('utf8'))
#                 print("Not LinkedIn results page, trying again")
#                 time.sleep(10)
#                 trys += 1
#
#         raise TypeError('Not LinkedIn Results Page')
#
#     def next_page(self, soup):
#         next_url = json.loads(soup.find('code', {'id': 'decoratedJobPostingsModule'}).string)['paging']['links']['next']
#         if next_url is not None:
#             self.current_url = 'https://www.linkedin.com/' + next_url
#             self.get_page()
#             soup = BeautifulSoup(self.current_html, 'lxml')
#         else:
#             self.current_url = next(self.next_query(self.keyword))
#             self.get_page()
#             soup = self.check_loaded()
#
#         return soup
#
#     def __iter__(self):
#         soup = self.check_loaded()
#         errors = 0
#         while soup is not None:
#             entries = json.loads(soup.find('code', {'id': 'decoratedJobPostingsModule'}).string)['elements']
#             for entry in entries:
#                 x = entry['decoratedJobPosting']
#                 yield entry['viewJobCanonicalUrl'], x['jobPosting']['id']
#
#             if errors == 20:
#                 self.current_url = next(self.next_query(self.keyword))
#                 self.get_page()
#                 soup = self.check_loaded()
#                 errors = 0
#                 continue
#
#             print("\nNext page\n")
#
#             soup = self.next_page(soup)


class IndeedCrawler(StaticCrawler):
    def __init__(self, keyword, logger, quick=False):
        search_url = 'http://www.indeed.com/jobs?q=' + '+'.join(keyword.lower().split(' '))
        StaticCrawler.__init__(self, site_name='Indeed', url=search_url, keyword=keyword, logger=logger)
        self.job_type = IndeedJob
        self.url_generator = self.url_generator(quick)
        self.soup = BeautifulSoup(self.current_html, 'lxml')
        self.current_page = 0

    def check_loaded(self):
        soup = BeautifulSoup(self.current_html, 'lxml')

        try:
            _ = soup.find('table', {'id': 'pageContent'}).string
        except AttributeError as a:
            print a
            return False

        return True

    def url_generator(self, quick=False):
        cities = ['Tulsa%2C+OK', 'Los+Angeles%2C+CA', 'Manhattan%2C+NY', 'San+Francisco%2C+CA', 'Austin%2C+TX', 'Seattle%2C+WA',
                  'Chicago%2C+IL', 'Washington%2C+DC', 'Walnut+Creek%2C+CA', 'Dallas%2C+TX', 'Atlanta%2C+GA&radius=100',
                  'Sacramento%2C+CA', 'Tulsa%2C+OK', 'San+Diego%2C', 'Orlando%2C', 'Miami%2C+FL', 'Hawaii', 'Houston%2C+TX',
                  'Philadelphia%2C+PA', 'Phoenix%2C+AZ', 'San+Antonio%2C+TX', 'Dallas%2C+TX', 'San+Jose%2C+CA', 'Jacksonville%2C+FL',
                  'Indianapolis%2C+IN', 'Utah', 'Nebraska', 'Maine', 'Boston%2C+MA', 'North+Carolina', 'Alabama', 'Missouri', 'New+Jersey',
                  'Alaska', 'Nevada', 'Arizona', 'New+Mexico', 'Montana', 'Washington+State']
        random.shuffle(cities)
        pages_checked = 0
        quick_append = 'sort=date&fromage=1' if quick else 'sort=date'
        for city_i, city in enumerate(cities):
            self.logger.info("{}% completed {}".format(str(float(city_i)/len(cities)*100), self.keyword))
            for j in range(20):
                pages_checked += 1
                url = 'http://www.indeed.com/jobs?q={}&l={}&{}&limit=50&start={}'.format("+".join(self.keyword.split(" ")), city, quick_append, str(j*50))
                self.current_page += 1
                yield url
            self.current_page = 0

        yield None

    def next_page(self):
        self.current_url = self.url_generator.next()

        if self.current_url is not None:
            self.get_page()
            self.soup = BeautifulSoup(self.current_html, 'lxml')
        else:
            self.complete()

        nav = self.soup.find_all('span', 'pn')
        if len(nav) > 0:
            if not self.soup.find_all('span', 'pn')[-1].string.startswith('Next'):
                for i in range(20 - self.current_page):
                    self.url_generator.next()

    def __iter__(self):
        self.next_page()
        while self.soup is not None:
            entries = self.soup.find_all('div', {'class': ['row', 'result']})
            for entry in entries:
                link = entry.find('a', {'data-tn-element': 'jobTitle'})
                remote_identifier = entry.find('h2', {'class': 'jobtitle'})
                if remote_identifier:
                    if not remote_identifier['id'] in self.jobs_scraped:
                        if remote_identifier is None:
                            remote_identifier = entry.find('a', {'class': 'jobtitle'})
                        if link['href'].startswith('/company') or link['href'].startswith('/cmp'):
                            self.jobs_scraped.add(remote_identifier['id'])
                            yield 'http://www.indeed.com' + link['href'], remote_identifier['id']
                        else:
                            self.jobs_scraped.add(remote_identifier['id'])
                            yield 'http://www.indeed.com/viewjob?jk={}'.format(entry['data-jk']),  remote_identifier['id']
            self.next_page()
