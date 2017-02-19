from crawlers import IndeedCrawler
import logging
import argparse
from uploaders import postgres_database
import random
if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--quick', dest='quick', action='store_true')

    args = parser.parse_args()

    logging.basicConfig(filename='scrape.log',
                        level=logging.INFO,
                        format='%(asctime)s %(message)s')
    logging.info('Started scrape')
    logging.basicConfig()

    cur = postgres_database.cursor()
    cur.execute("SELECT * FROM scrapes")
    scrapes = cur.fetchall()
    random.shuffle(scrapes)

    for scrape in scrapes:
        logging.info('Started {}'.format(scrape[2]))
        jobs = IndeedCrawler(scrape[1], logging, args.quick)
        jobs.post_jobs()

