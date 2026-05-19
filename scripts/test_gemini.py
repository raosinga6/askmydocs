"""Verify Gemini API access works before generating 499 narratives."""
import os
from dotenv import load_dotenv
from google import genai

load_dotenv()

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
resp = client.models.generate_content(
    model="gemini-2.5-pro",
    contents="In one sentence: what is a data dictionary?",
)
print(resp.text)