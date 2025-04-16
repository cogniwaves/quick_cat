#!/usr/bin/env python3
import os
import time
import psycopg2
from psycopg2.extras import execute_values, execute_batch
from tqdm import tqdm
import voyageai
from dotenv import load_dotenv

load_dotenv()

# Set up Voyage AI client
api_key = os.environ.get("VOYAGE_API_KEY")
vo = voyageai.Client(api_key=api_key)

# Set up Supabase DB connection
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DB_URL = f"{SUPABASE_URL}:{SUPABASE_KEY}@aws-0-ca-central-1.pooler.supabase.com:5432/postgres"
print(DB_URL)

def create_embeddings(text):
    contextualized_item = f"food item: {text}"
    result = vo.embed(contextualized_item, model="voyage-3-large", input_type="document")
    return result.embeddings[0]

def process_batch(conn, batch_size, sleep_time):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT subcategory_id, searchable_text FROM subcategory2 WHERE voyage_1024 IS NULL AND searchable_text IS NOT  NULL LIMIT %s",
            (batch_size,)
        )
        rows = cur.fetchall()

    if not rows:
        return 0

    updates = []
    for subcategory_id, text in tqdm(rows, desc="Processing batch", leave=False):
        embedding = create_embeddings(text)
        updates.append((embedding, subcategory_id))
        time.sleep(sleep_time)

    with conn.cursor() as cur:
        execute_batch(cur,
            "UPDATE subcategory2 SET voyage_1024 = %s WHERE subcategory_id = %s",
            updates 
        )
    conn.commit()
    return len(rows)

def main():
    conn = psycopg2.connect(DB_URL)

    batch_size = 100
    sleep_time = 0.03  # ~33 requests/sec

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM subcategory2 WHERE voyage_1024 IS NULL")
        total_remaining = cur.fetchone()[0]

    pbar = tqdm(total=total_remaining, desc="Total Progress")
    while True:
        processed = process_batch(conn, batch_size, sleep_time)
        if processed == 0:
            break
        pbar.update(processed)
    pbar.close()
    conn.close()

if __name__ == "__main__":
    main()



