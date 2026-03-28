"""
Configuration service for ETL Multi-Agent System
Loads environment variables from .env file
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from the same directory as this script
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Validation
if not OPENAI_API_KEY:
    raise ValueError(
        "OPENAI_API_KEY not found!\n"
        f"   Please add your API key to: {env_path}\n"
        "   Example: OPENAI_API_KEY=sk-proj-xxxxx"
    )

print(f"Configuration loaded from {env_path}")
print(f"API Key: {OPENAI_API_KEY[:20]}..." if len(OPENAI_API_KEY) > 20 else "API Key loaded")
