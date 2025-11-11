import os
import csv
from pymongo import MongoClient
import pandas as pd

def main(path_out, db_name='pubdb', collection_name='papers'):
    '''
    Connect to MongoDB and return the collection object.
    '''
    # check for MONGODB_URI environment variable
    if 'MONGODB_URI' not in os.environ:
        raise EnvironmentError('MONGODB_URI environment variable not set. e.g., mongodb://user:pass@host:port')
    
    client = MongoClient(os.getenv('MONGODB_URI'))
    print(f"* connected to MongoDB {os.getenv('MONGODB_URI')}")

    db = client[db_name][collection_name]
    print(f"* using database: {db_name}, collection: {collection_name}")

    # get all papers
    papers = db.find()

    # save the papers to a tsv file
    print('* converting papers to DataFrame and saving to TSV file...')
    df = pd.DataFrame(papers)

    print(f'* saving to {path_out} ...')
    df.to_csv(
        path_out,
        sep='\t',
        index=False,
        quoting=csv.QUOTE_NONE
    )
    print(f"* dumped {len(df)} papers to {path_out}")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Dump publications database to a TSV file.')
    parser.add_argument('path_out', type=str, help='Output path for the TSV file')
    args = parser.parse_args()
    main(args.path_out)