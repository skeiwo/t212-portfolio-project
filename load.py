from dotenv import load_dotenv
from supabase import create_client, Client
import os
import pandas as pd

load_dotenv()

SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

def load_to_db(df: pd.DataFrame) -> None:
    data = df.to_dict(orient = "records")
    try:
        response = supabase.table("t212_positions").insert(data).execute()
        print("Data loaded succesfully")
    except Exception as e:
        raise Exception(f"Error: {str(e)}")