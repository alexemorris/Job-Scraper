# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

from secret import *
import datetime
import psycopg2


class DBConnection(object):
    def __init__(self):
        self.reconnect()

    def reconnect(self):
        connection_str = "dbname='{DBNAME}' user='{USER}' host='{HOST}' password='{PASSWORD}'"
        self.conn = psycopg2.connect(connection_str.format(DBNAME=POSTGRES_DBNAME,
                                                           USER=POSTGRES_USERNAME,
                                                           HOST=POSTGRES_HOST,
                                                           PASSWORD=POSTGRES_PASSWORD))

postgres_database = DBConnection().conn


class PostgresJob(object):
    def __init__(self, crawler, logger):
        self.conn = postgres_database
        self.cursor = self.conn.cursor()
        self.site_name = crawler.site_name
        self.query_title = crawler.keyword
        self.logger = logger
        self.buffer = 0
        self.query = []

    def check_id_existence(self, job_id, attempt=0):
        sql_query = """
          SELECT job_id FROM jobs_full WHERE remote_identifier = %s and query_title = %s
        """
        try:
            self.cursor.execute(sql_query, (job_id, self.query_title))
        except psycopg2.InterfaceError, psycopg2.OperationalError:
            if attempt < 5:
                attempt += 1
                postgres_database.reconnect()
                self.check_id_existence(job_id, attempt)
            else:
                raise psycopg2.DatabaseError('Too many trys to reconnect to DB')
        if isinstance(self.cursor.fetchone(), tuple):
            return True
        else:
            return False

    def post_job(self, job):
        if self.buffer < 50:
            # if not self.check_id_existence(job.remote_identifier):
            job_id = job.uuid
            job_name = job.title
            scraping_url = job.url
            company = job.company_name
            scraping_date = datetime.datetime.now()
            posting_date = job.posting_date
            company_id = job.company_id
            remote_identifier = job.remote_identifier
            meta = job.to_json()
            data = (job_id, job_name, scraping_url, company, scraping_date, remote_identifier, company_id,
                    posting_date, self.query_title, meta, self.site_name)

            sql_query = """
              INSERT INTO jobs_full(job_id, job_name, scraping_url, company, scraping_date, remote_identifier,
              company_remote_identifier,  posting_date, query_title, meta, website)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.query.append(self.cursor.mogrify(sql_query, data))
            self.buffer += 1

        else:
            print self.query[1:10]
            try:
                self.cursor.execute(";".join(self.query))
            except psycopg2.DataError as a:
                self.conn.rollback()
                self.logger.error("error posting, {}".format(str(a)))
            except psycopg2.InternalError as a:
                self.conn.rollback()
                self.logger.error("error posting, {}".format(str(a)))
            self.query = []
            self.buffer = 0

    def close_connection(self):
        self.cursor.close()
        self.conn.close()
#
#
# class S3Job(object):
#     """ given a name and contents uploads a job description to the s3 bucket """
#
#     def __init__(self):
#         self.s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
#
#     def post_job(self, job):
#         self.s3.put_object(Bucket=JOB_DESCRIPTION_S3, Key='{}.json'.format(job.uuid), Body=job.to_json())
