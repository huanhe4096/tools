import sqlite3
import pandas as pd
from tqdm import tqdm
import argparse

def main(
    full_fn_csv,
    full_fn_sqlite,
    table_name="papers"
):
    '''Import iCite data CSV file into a SQLite 
    '''
    conn = sqlite3.connect(full_fn_sqlite)
    cursor = conn.cursor()

    # Read CSV in chunks and insert into the database
    chunk_size = 4096 
    print(f'* creating the icite db with chunk size {chunk_size}... ')
    for chunk in tqdm(pd.read_csv(full_fn_csv, chunksize=chunk_size)):
        chunk.to_sql(name=table_name, con=conn, if_exists="append", index=False)
    print('* inserted all records!')

    # Commit and close the connection
    create_index_queries = [
        'create index idx_pmid on papers (pmid)',
        'create index idx_year on papers (year)',
        'create index idx_cc on papers (citation_count)',
        'create index idx_rcr on papers (relative_citation_ratio)',
    ]

    for query in create_index_queries:
        print('* executing query:', query)
        cursor.execute(query)

    conn.commit()
    conn.close()
    print('* done!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Import all iCite data to a SQLite db")
    parser.add_argument("icite_csv", help="full path to iCite CSV file")
    parser.add_argument("sqlite_db", help="full path to SQLite db")
    
    args = parser.parse_args()

    main(
        args.icite_csv,
        args.sqlite_db,
    )
