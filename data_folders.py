"""Store of key folders and how to treaty data attributes"""
import os

XEFR = os.path.join('data', 'xefr')
TSP = os.path.join('data', 'tsp')
BULLHORN = os.path.join('data', 'bullhorn')
XEFR_UPLOADS = os.path.join('data', 'xefr', 'uploads')
TEST_UPLOADS = os.path.join('data', 'test')

sort_orders = {
    'Metrics UK NFI Adjustments':'Candidate,Placement,InvoiceDate',
    'TSP UK Invoice Tracker':'Candidate,Placement,InvoiceDate',
    'Metrics GBP Forex Daily':'Year,Month,Day,Symbol',
    'Bullhorn Candidates':'Consultant',
    'Bullhorn Consultant Details':'Consultant'
}