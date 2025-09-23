#%%

import os
import dotenv
import requests
import json

#%%

dotenv.load_dotenv()

DATBRICKS_TOKEN = os.getenv("tokendatabricks")
DATABRICKS_HOST = os.getenv("host")

#%%

url = f"https://{DATABRICKS_HOST}/api/2.1/jobs/list"
header = {"Authorization": f"Bearer {DATBRICKS_TOKEN}"}

response = requests.get(url, headers=header)
jobs = response.json()

#%%

jobs["jobs"][0]

# %%
