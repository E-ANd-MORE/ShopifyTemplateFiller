"""
Configuration settings for Product Enrichment Pipeline
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
CACHE_DIR = PROJECT_ROOT / "cache"
LOGS_DIR = PROJECT_ROOT / "logs"

# Create directories if they don't exist
for directory in [INPUT_DIR, OUTPUT_DIR, CACHE_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# API Keys
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# API Configuration
API_CONFIG = {
    "tavily": {
        "endpoint": "https://api.tavily.com/search",
        "timeout": 30,
        "max_retries": 3,
        "rate_limit_delay": 0.2,  # Reduced from 0.5s to 0.2s (not used in current flow)
        "max_results": 10,  # Get more results to filter out login pages
        "search_depth": "advanced",  # Use advanced search for better results
    },
    "firecrawl": {
        "endpoint": "https://api.firecrawl.dev/v1/scrape",
        "timeout": 45,
        "max_retries": 3,
        "rate_limit_delay": 0.3,  # Reduced from 1.0s to 0.3s (not used in current flow)
        "include_tags": ["img"],  # Focus on images
        "wait_for": 2000,  # Wait 2s for page to load images
    },
    "claude": {
        "model": "claude-haiku-4-5-20251001",
        "temperature": 0,  # Deterministic
        "max_tokens": {
            "variants": 500,
            "description": 300,
            "category": 50,
            "tags": 200,
            "clean_name": 100,
        }
    }
}

# Processing Configuration
PROCESSING_CONFIG = {
    "batch_size": int(os.getenv("BATCH_SIZE", 100)),  
    "max_workers": int(os.getenv("MAX_WORKERS", 5)),  # Increase from 5 to 10 for parallel processing
    "enable_checkpoints": True,
    "checkpoint_interval": 1,  # Save after each batch
    "records_per_file": int(os.getenv("RECORDS_PER_FILE", 1000)),  # Maximum records per output CSV
    "parallel_enrichment": True,  # Enable parallel Claude API calls
}

# Domain Priority for URL Search (Brand website FIRST, then retailers)
DOMAIN_PRIORITY = [
    "{brand_domain}.com",
    "{brand_domain}.sa",
    "{brand_domain}.ae",
    "{brand_domain}.co",
    "iherb.sa",
    "sa.iherb.com",
    "iherb.com",
    "amazon.sa",
    "noon.com",
    "namshi.com",
    "amazon.ae",
]

# Shopify Product Categories
# Shopify Standard Product Taxonomy Categories
# Format: Hierarchical with ">" separator (minimum 2 levels recommended)
# Source: https://shopify.github.io/product-taxonomy/releases/unstable/
# Top level: "Health & Beauty" (as per official taxonomy)
SHOPIFY_CATEGORIES = [
    "Health & Beauty > Hair Care",
    "Health & Beauty > Skin Care",
    "Health & Beauty > Makeup",
    "Health & Beauty > Bath & Body",
    "Health & Beauty > Oral Care",
    "Health & Beauty > Fragrance",
    "Health & Beauty > Nail Care",
    "Health & Beauty > Personal Care",
    "Home & Garden > Home Decor",
]

# Logging Configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "detailed": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "simple": {
            "format": "%(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "simple",
            "stream": "ext://sys.stdout"
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "DEBUG",
            "formatter": "detailed",
            "filename": str(LOGS_DIR / "pipeline.log"),
            "mode": "a"
        }
    },
    "loggers": {
        "": {  # Root logger
            "level": "DEBUG",
            "handlers": ["console", "file"]
        }
    }
}

# Validation Rules
VALIDATION_RULES = {
    "handle_max_length": 255,
    "title_max_length": 255,
    "description_max_length": 5000,
    "tag_max_length": 50,
    "tag_min_count": 6,
    "tag_max_count": 10,
    "price_min": 0,
    "price_max": 999999,
    "max_images_per_product": 3,
}

# Product Grouping Configuration
GROUPING_CONFIG = {
    "similarity_threshold": 0.7,  # For fuzzy matching product names
    "group_by_brand": True,
    "preserve_all_variants": True,
}
