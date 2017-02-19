import re
from psycopg2 import DataError
from uploaders import postgres_database
import logging

def convert_to_int(num_string):

    number_range = re.compile(r'\b([0-9])\s*-\s*([0-9])')
    e_range = number_range.match(num_string)

    if e_range:
        try:
            return (float(e_range.group(1)) + float(e_range.group(2)))/2
        except TypeError:
            return None

    stripped = num_string.replace('+', '')

    try:
        return float(stripped)
    except ValueError:
        try:
            return number_words[stripped]
        except KeyError:
            raise TypeError("not value number")

if __name__ == '__main__':
    conn = postgres_database
    cursor = conn.cursor()

    logging.basicConfig(filename='experience.log',
                        level=logging.INFO,
                        format='%(asctime)s %(message)s')


    logging.info('Started experience parsing')
    logging.basicConfig()

    number_words = {
        'one': 1.0, 'two': 2.0, 'three': 3.0, 'four': 4.0, 'five': 5.0, 'six': 6.0, 'seven': 7.0, 'eight': 8.0,
        'nine': 9.0, 'ten': 10.0, 'fifteen': 15.0, 'twenty': 20.0
    }

    select_query = """
         SELECT jobs_full.job_id , meta ->>'description' from jobs_full WHERE jobs_full.job_id NOT IN (
          SELECT experience.job_id from experience)
    """

    cursor.execute(select_query)
    jobs = cursor.fetchall()
    logging.info('Collected jobs')

    experience = re.compile(r'.{50}experience')
    number = re.compile(r'\b([0-9]\s*-\s*[0-9]|[0-9]{1,2}\+*|one|two|three|four|five|six|seven|'
                        r'eight|nine|ten|fifteen|twenty)\s+year(?:s)*')
    number_range = re.compile(r'\b([0-9])\s*-\s*([0-9])')
    out_values = []

    for x in jobs:
        for y in experience.findall(x[1]):
            numbers = number.findall(y.lower())
            if len(numbers) == 1:
                e = numbers[0]
                out = convert_to_int(e)
                if out <= 25:
                    out_values.append((x[0], out))
                    break

    logging.info('Parsed experience values')

    insert_query = "INSERT INTO experience(job_id, experience) VALUES (%s, %s)"

    for entry in out_values:
        try:
            cursor.execute(insert_query, entry)
            conn.commit()
        except DataError as a:
            conn.rollback()
            print entry
