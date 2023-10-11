"""Used simply as a script to test the query module."""
import os
import sys
import utilities
import etl_common
import ast
import etl_tsp

TSP_DATA_DIR = os.path.join('data', 'tsp')
TEST_DATA_DIR = os.path.join('data', 'test')
BULL_DATA_DIR = os.path.join('data', 'bullhorn')
XEFR_DATA_DIR = os.path.join('data', 'xefr')
RECONCILIATION_DIR = os.path.join('data', 'reconciliations')

logging = utilities.MyLogger()
logger = logging.getLogger()
tsp = etl_tsp.TSP_ETL('TSP_US',TSP_DATA_DIR, logger)

def main():

    charge_codes = ['CON-123-E1','CANRET2','PERM-RETCAN','1647','PERM-AXRETJUN','MAHRET2']

    for i in charge_codes:
        print(f"{i} is: {tsp._TSP_ETL__tsp_format_placement(i)}")


if __name__ == "__main__":
    logging.reset_log()
    main()
