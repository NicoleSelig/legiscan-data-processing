import zipfile
import base64
import io
import pandas as pd
import glob2
import os
import requests
import sqlalchemy
from tqdm import tqdm
import swifter
from legiscan import LegiScan
from data_processors import process_bill
from data_processors import process_person
from data_processors import process_session
from data_processors import process_sponsor
from data_processors import process_vote
from multiprocessing import Pool

api_key = os.environ.get('LEGISCAN_API_KEY')
legis = LegiScan(api_key)

datasets = []
states = ['IN', 'US']
# print('Getting List of Datasets...')
# for state in tqdm(states):
#     dataset = legis.get_dataset_list(state)
#     datasets.extend(dataset)

# def download_dataset(dataset):
#     dataset_data = legis.get_dataset(dataset['session_id'], dataset['access_key'])
#     z_bytes = base64.b64decode(dataset_data['zip'])
#     z = zipfile.ZipFile(io.BytesIO(z_bytes))
#     z.extractall("./sample-data")

# print('Downloading Dataset JSON Data...')
# with Pool(len(datasets)) as p:
#     p.map(download_dataset, datasets)

tables_to_process = [
    ['bill', 'bills', process_bill], 
    ['bill', 'sessions', process_session], 
    ['bill', 'sponsors', process_sponsor], 
    ['vote', 'votes', process_vote],
    ['people', 'people', process_person]
]

def process_table(tables_to_process):
    tqdm.pandas()
    dirname = tables_to_process[0]
    table = tables_to_process[1]
    process_fn = tables_to_process[2]

    print(f'Processing {table}')
    filenames = glob2.glob(f"sample-data/*/*/{dirname}/*.json")
    bill_df = pd.Series(filenames).progress_apply(process_fn)
    bill_df.to_csv(f'{table}.csv', index=False)

with Pool(len(tables_to_process)) as p:
    p.map(process_table, tables_to_process)


# bill_filenames = glob2.glob("sample-data/*/2010-2010_Regular_Session/bill/HB1001.json")
# process_session(bill_filenames[0])
# # vote_filenames = glob2.glob("sample-data/*/*/vote/*.json")
# # people_filenames = glob2.glob("sample-data/*/*/people/*.json")

# bill_df = pd.Series(people_filenames).swifter.apply(process_person)
# bill_df.to_sql("people.csv", index=False)

# bill_df = pd.Series(bill_filenames).swifter.apply(process_bill)
# bill_df.to_csv("bills.csv", index=False)

# session_df = pd.Series(bill_filenames).swifter.apply(process_session)
# session_df.to_csv("sessions.csv", index=False)

# sponsor_df = pd.Series(bill_filenames).swifter.apply(process_sponsor)
# sponsor_df.to_csv("sponsors.csv", index=False)

# votes_df = pd.Series(vote_filenames).swifter.apply(process_votes)
# votes_df.to_csv("votes.csv", index=False)


