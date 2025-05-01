import openai
from dotenv import load_dotenv
import os
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

models = openai.Model.list()
for model in models['data']:
    print(model['id'])
