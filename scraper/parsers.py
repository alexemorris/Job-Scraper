from bs4 import BeautifulSoup
import re
import json
import datetime
import uuid

# JOB_DESCRIPTION_S3 = 'job-description-files'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11) AppleWebKit/601.1.56 (KHTML, like Gecko) Version/9.0 Safari/601.1.56',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)


class GeneralParser(object):
    """General structure of a parser object, all fields should be filled in. Data types to be parsed are
    description: Job description text including HTML tags
    title: Job title string
    company name: Company name string
    postingDate: datetime object of time job was posted
    meta: JSON serializable dictionary of additional metadata
    """

    def __init__(self, url, meta=None, title=None, company_name=None, posting_date=None, location=None,
                 remote_identifier=None, company_id=None):
        self.url = url
        self.description = None
        self.title = title
        self.company_name = company_name
        self.location = location
        self.posting_date = posting_date
        self.uuid = str(uuid.uuid4())
        self.remote_identifier = remote_identifier
        self.company_id = company_id

        if meta is None:
            self.meta = {}
        else:
            self.meta = meta

    def to_json(self):
        output = {
            'url': self.url,
            'description': self.description,
            'title': self.title,
            'company_name': self.company_name,
            'location': self.location,
            'posting_date': self.posting_date,
            'uuid': self.uuid,
            'meta': self.meta,
            'company_id': self.company_id,
        }

        return json.dumps(output, sort_keys=False, separators=(',', ': '), cls=DateTimeEncoder)


class StaticParser(GeneralParser):
    """
    This is required for job postings where content is retrieved dynamically by the browser, slower than static parser.
    """

    def __init__(self, url, session, *args, **kwargs):
        GeneralParser.__init__(self, url, *args, **kwargs)
        self.s = session
        self.html = None
        self.get_page()

    def get_page(self):
        self.html = self.s.get(self.url, headers=HEADERS).content

    def check_loaded(self):
        return True


# class DynamicParser(GeneralParser):
#     """
#     This is required for job postings where content is retrieved dynamically by the browser, MUCH slower than static
#     parser. It takes a selenium webdriver to eliminate overhead of starting browser when parsing
#     """
#
#     def __init__(self, url, driver, *args, **kwargs):
#         GeneralParser.__init__(self, url, *args, **kwargs)
#         self.d = driver
#         self.d.get(url)
#         self.html = self.d.execute_script("return document.getElementsByTagName('html')[0].innerHTML")
#
#
# class LinkedInJob(StaticParser):
#     def __init__(self, url, session, *args, **kwargs):
#         StaticParser.__init__(self, url, session, *args, **kwargs)
#         self.parse()
#
#     def check_loaded(self):
#         soup = BeautifulSoup(self.html, 'lxml')
#         try:
#             code = soup.find('code', {"id": "jobDescriptionModule"})
#             html_desc = json.loads(code.string)
#         except AttributeError as a:
#             raise TypeError('Not LinkedIn Posting')
#
#         return True
#
#     @staticmethod
#     def get_json(json, route):
#         current_json = json
#         for item in route:
#             try:
#                 current_json = current_json.get(item)
#             except AttributeError as a:
#                 return None
#         return current_json
#
#     def parse(self):
#         self.check_loaded()
#         soup = BeautifulSoup(self.html, 'lxml')
#         empty_tags = r'<(\w+)>\s*(?:&nbsp;)*\s*<\/\1>'
#         code = soup.find('code', {"id": "jobDescriptionModule"})
#         html_desc = json.loads(code.string)
#
#         self.description = re.sub(empty_tags, "", html_desc['description'])
#         json_desc = json.loads(soup.find('code', {"id": "decoratedJobPostingModule"}).string).get('decoratedJobPosting')
#
#         self.company_id = self.get_json(json_desc, ('decoratedCompany', 'company', 'companyId'))
#
#         if self.company_id is None:
#             self.company_id = 0
#
#         self.title = self.get_json(json_desc, ('jobPosting', 'title'))
#
#         self.remote_identifier = self.get_json(json_desc, ('jobPosting', 'id'))
#
#         self.company_name = self.get_json(json_desc, ('jobPosting', 'companyName'))
#         self.posting_date = datetime.datetime.fromtimestamp(
#             int(self.get_json(json_desc, ('jobPosting', 'listDate'))) / 1000)
#         self.meta['company_description'] = self.get_json(json_desc, ('jobPosting', 'companyDescription', 'rawText'))
#         self.meta['countryCode'] = self.get_json(json_desc, ('jobPosting', 'countryCode'))
#         self.meta['functions'] = json_desc.get('formattedJobFunctions')
#         self.meta['industries'] = json_desc.get('formattedIndustries')
#         self.meta['postalCode'] = self.get_json(json_desc, ('jobPosting', 'postalCode'))
#         self.meta['experience'] = json_desc.get('formattedExperience')
#         self.meta['contractType'] = json_desc.get('formattedEmploymentStatus')


class IndeedJob(StaticParser):
    def __init__(self, url, session, *args, **kwargs):
        StaticParser.__init__(self, url, session, *args, **kwargs)
        self.parse()

    def check_loaded(self):
        return True

    def parse(self):
        self.check_loaded()
        soup = BeautifulSoup(self.html, 'lxml')
        posting = soup.find('span', {'id': 'job_summary'})
        self.description = "".join(str(x) for x in posting.contents)
        title = soup.find('b', {'class': 'jobtitle'}).text
        self.title = title

        company = soup.find('span', {'class': 'company'}).string
        self.company_name = company
        self.company_id = 0
        location = soup.find('span', {'class': 'location'}).string
        self.location = location
        date = int(re.search(r'\d+', soup.find('span', {'class': 'date'}).string).group(0))
        today = datetime.datetime.today()
        self.posting_date = today - datetime.timedelta(days=date)
