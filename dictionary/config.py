"""
Configuration service for ETL Multi-Agent System
Loads environment variables from .env file
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from the same directory as this script
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

# API Configuration — supports Anthropic, Google Gemini, or OpenAI
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Determine which provider is available (priority: Anthropic > Google > OpenAI)
if ANTHROPIC_API_KEY:
    AI_PROVIDER = "anthropic"
    API_KEY = ANTHROPIC_API_KEY
    print(f"Configuration loaded from {env_path}")
    print(f"AI Provider: Anthropic (Claude)")
elif GOOGLE_API_KEY:
    AI_PROVIDER = "gemini"
    API_KEY = GOOGLE_API_KEY
    print(f"Configuration loaded from {env_path}")
    print(f"AI Provider: Google Gemini")
elif OPENAI_API_KEY:
    AI_PROVIDER = "openai"
    API_KEY = OPENAI_API_KEY
    print(f"Configuration loaded from {env_path}")
    print(f"AI Provider: OpenAI")
else:
    raise ValueError(
        "No API key found!\n"
        f"   Please add your API key to: {env_path}\n"
        "   Example: ANTHROPIC_API_KEY=sk-ant-...\n"
        "   Or:      GOOGLE_API_KEY=AIzaSy...\n"
        "   Or:      OPENAI_API_KEY=sk-proj-..."
    )
