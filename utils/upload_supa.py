import os
from tqdm import tqdm
import glob
import sys
from supabase import create_client
import mimetypes



# Supabase configuration
supabase_url = "https://oqvdwtiwrzyjpnouwxch.supabase.co"
supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9xdmR3dGl3cnp5anBub3V3eGNoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0MTk3NjQyMywiZXhwIjoyMDU3NTUyNDIzfQ.PwN4fxZVJKPaX_HiLOVdP0YjqvnZJ3ZqaRP7g8UowzA"
bucket_name = "catalog-images"
subdirectory = "saq"

supabase = create_client(supabase_url, supabase_key)

def upload_file(file_path: str):
    file_name = os.path.basename(file_path)
    object_path = f"{subdirectory}/{file_name}"
    file_size = os.path.getsize(file_path)

    mime_type, _ = mimetypes.guess_type(file_path)
    mime_type = mime_type or "application/octet-stream"

    with open(file_path, "rb") as f:
        file_bytes = f.read()

    with tqdm(total=file_size, unit="B", unit_scale=True, desc=file_name) as pbar:
        response = (
            supabase.storage
            .from_(bucket_name)
            .upload(
                file=file_bytes,
                path=object_path,
                file_options={"cache-control": "3600", "upsert": "false", "content-type": mime_type}
            )
        )
        pbar.update(file_size)

    print(f"Uploaded {file_path} to {object_path} ({mime_type}): {response}")

    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python upload.py '<wildcard_pattern>'")
        sys.exit(1)

    file_pattern = sys.argv[1]
    files = glob.glob(file_pattern)

    if not files:
        print(f"No files found matching the pattern: {file_pattern}")
        sys.exit(1)

    for file_path in files:
        print(f"uploading {file_path}")
        upload_file(file_path)