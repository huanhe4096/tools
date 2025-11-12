import os
import csv
from pymongo import MongoClient
from tqdm import tqdm

def process_document(doc):
    '''
    Process a single document: convert lists to comma-separated strings,
    drop unwanted fields, and escape special characters in abstract.
    '''
    # Drop full_text and full_text_type fields
    doc.pop('full_text', None)
    doc.pop('full_text_type', None)

    # Rename publication_date to pubdate
    if 'publication_date' in doc:
        doc['pubdate'] = doc.pop('publication_date')

    # Rename source to journal
    if 'source' in doc:
        doc['journal'] = doc.pop('source')

    # Convert mesh_terms list to comma-separated string
    if 'mesh_terms' in doc and isinstance(doc['mesh_terms'], list):
        doc['mesh_terms'] = ';'.join(str(term) for term in doc['mesh_terms'])

    # Convert authors list to comma-separated string
    if 'authors' in doc and isinstance(doc['authors'], list):
        doc['authors'] = ';'.join(str(author) for author in doc['authors'])

    # Convert references list to comma-separated string
    if 'references' in doc and isinstance(doc['references'], list):
        doc['references'] = ';'.join(str(ref) for ref in doc['references'])

    # Escape newlines and tabs in abstract
    if 'abstract' in doc and isinstance(doc['abstract'], str):
        doc['abstract'] = doc['abstract'].replace('\n', '\\n').replace('\t', '\\t')

    return doc

def main(path_out, db_name='pubdb', collection_name='papers', batch_size=10000):
    '''
    Connect to MongoDB and stream documents to TSV file in batches.
    '''
    # check for MONGODB_URI environment variable
    if 'MONGODB_URI' not in os.environ:
        raise EnvironmentError('MONGODB_URI environment variable not set. e.g., mongodb://user:pass@host:port')

    client = MongoClient(os.getenv('MONGODB_URI'))
    print(f"* connected to MongoDB {os.getenv('MONGODB_URI')}")

    db = client[db_name][collection_name]
    print(f"* using database: {db_name}, collection: {collection_name}")

    # Create projection to exclude 'raw', '_id', 'full_text', and 'full_text_type' fields
    projection = {'raw': 0, '_id': 0, 'full_text': 0, 'full_text_type': 0}

    # Get estimated count for progress bar (much faster than count_documents)
    estimated_total = db.estimated_document_count()
    print(f'* streaming ~{estimated_total:,} documents in batches of {batch_size}...')

    # Get cursor for streaming (excluding 'raw' and '_id' fields)
    cursor = db.find({}, projection)

    # Open file and write in batches
    print(f'* saving to {path_out} ...')
    with open(path_out, 'w', newline='', encoding='utf-8') as f:
        writer = None
        batch = []
        total_count = 0

        # Initialize progress bar with estimated total
        pbar = tqdm(total=estimated_total, unit='docs', desc='Saving documents')

        for doc in cursor:
            # Process document
            doc = process_document(doc)

            batch.append(doc)

            # Write batch when it reaches batch_size
            if len(batch) >= batch_size:
                if writer is None:
                    # Initialize writer with fieldnames from first document
                    fieldnames = batch[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
                    writer.writeheader()

                writer.writerows(batch)
                total_count += len(batch)
                pbar.update(len(batch))
                batch = []

        # Write remaining documents
        if batch:
            if writer is None:
                # Initialize writer if we only have one small batch
                fieldnames = batch[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()

            writer.writerows(batch)
            total_count += len(batch)
            pbar.update(len(batch))

        pbar.close()

    print(f"* dumped {total_count:,} papers to {path_out}")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Dump publications database to a TSV file.')
    parser.add_argument('path_out', type=str, help='Output path for the TSV file')
    args = parser.parse_args()
    main(args.path_out)