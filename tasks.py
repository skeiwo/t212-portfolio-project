from datetime import datetime
from dotenv import load_dotenv
from prefect import task
from supabase import create_client, Client

import os
import random
import time

load_dotenv()

SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)


@task
def get_numbers() -> list[int]:
    time.sleep(3)
    print("Extracted numbers")
    return [random.randint(1,5) for _ in range(0, 5)]

@task
def transform_numbers(numbers: list[int]) -> list[int]:
    time.sleep(3)
    print("Transformed numbers")
    return sorted([number**2 for number in numbers])

@task
def load_numbers(transformed_numbers: list[int]) -> None:
    data = {"created_at": datetime.now().strftime("%d/%m/%Y, %H:%M:%S"), "numbers": transformed_numbers}
    try:
        response = supabase.table("users").insert(data).execute()
        print("Data loaded succesfully")
    except Exception as e:
        raise Exception(f"Error: {str(e)}")
    
    response = supabase.table("users").select("*").execute()
    return response.data