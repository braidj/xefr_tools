"""Handle ETL of manual files"""
import os
import sys
import utilities
MANUAL_DATA_DIR = os.path.join('data', 'manual')
logging = utilities.MyLogger()
logger = logging.getLogger()

def main():
    etl_file('bullhorn placements')

def etl_file(file_name):
    """ETL the file for storage in XEFR"""

    logger.debug('etl_file called: %s', locals())

    utilities.csv_convert_dates(MANUAL_DATA_DIR,file_name,['startDate','endDate'],'%d/%m/%Y','%Y-%m-%d',logger)

if __name__ == '__main__':
    main()