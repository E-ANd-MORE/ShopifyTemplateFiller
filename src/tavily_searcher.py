"""
Module 2: Tavily Searcher
Search for product URLs using Tavily API.
"""
import logging
import time
import json
import hashlib
import requests
from typing import Optional, Dict
from pathlib import Path
from urllib.parse import urlparse

from config import TAVILY_API_KEY, API_CONFIG, CACHE_DIR, DOMAIN_PRIORITY

logger = logging.getLogger(__name__)


class TavilySearcher:
    """
    Search for product URLs using Tavily API.
    
    Features:
    - Domain prioritization (brand sites → retailers)
    - Exponential backoff retry logic
    - JSON caching with auto-save
    - Rate limiting
    - URL validation
    """
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or TAVILY_API_KEY
        if not self.api_key:
            raise ValueError("Tavily API key is required")
        
        self.config = API_CONFIG['tavily']
        self.endpoint = self.config['endpoint']
        self.timeout = self.config['timeout']
        self.max_retries = self.config['max_retries']
        self.rate_limit_delay = self.config['rate_limit_delay']
        self.max_results = self.config['max_results']
        
        # Cache setup
        self.cache_file = Path(CACHE_DIR) / 'tavily_cache.json'
        self.cache = self._load_cache()
        
        logger.info("TavilySearcher initialized")
    
    def search_url(self, brand: str, product_name: str) -> Optional[str]:
        """
        Search for product URL using Tavily API.
        
        Args:
            brand: Product brand name
            product_name: Product name
            
        Returns:
            Valid HTTPS URL if found, None otherwise
        """
        # Input validation
        if not brand or not product_name:
            logger.warning("Missing brand or product name")
            return None
        
        brand = brand.strip()
        product_name = product_name.strip()
        
        # Check cache first
        cache_key = self._generate_cache_key(brand, product_name)
        cached_url = self.cache.get(cache_key)
        
        if cached_url:
            logger.debug(f"Cache hit: {brand} - {product_name}")
            return cached_url
        
        # Construct query - more specific to get product pages
        query = f"{brand} {product_name} buy product page"
        logger.info(f"Searching: {query}")
        
        # Generate domain list
        domains = self._get_priority_domains(brand)
        
        # API call with retry
        for attempt in range(1, self.max_retries + 1):
            try:
                payload = {
                    "api_key": self.api_key,
                    "query": query,
                    "max_results": self.max_results,
                    "include_domains": domains,
                    "search_depth": self.config.get('search_depth', 'basic')
                }
                
                response = requests.post(
                    self.endpoint,
                    json=payload,
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    data = response.json()
                    results = data.get('results', [])
                    
                    if results:
                        # Filter out login/account pages
                        filtered_results = [
                            r for r in results
                            if not any(skip in r.get('url', '').lower() 
                                     for skip in ['login', 'signin', 'account', 'cart', 'checkout', 'register'])
                        ]
                        
                        if filtered_results:
                            url = filtered_results[0].get('url', '')
                        elif results:
                            # Fallback to first result if all filtered out
                            url = results[0].get('url', '')
                        else:
                            url = ''
                        
                        # Validate URL
                        if url and self._validate_url(url):
                            self.cache[cache_key] = url
                            self._save_cache()
                            logger.info(f"✓ Found: {url}")
                            
                            # Rate limiting
                            time.sleep(self.rate_limit_delay)
                            return url
                    
                    logger.debug(f"No results for: {query}")
                    self.cache[cache_key] = None
                    self._save_cache()
                    return None
                
                elif response.status_code == 429:
                    wait_time = 2 ** attempt
                    logger.warning(f"Rate limited. Waiting {wait_time}s (attempt {attempt}/{self.max_retries})...")
                    time.sleep(wait_time)
                    continue
                
                else:
                    logger.error(f"API error {response.status_code}: {response.text[:200]}")
                    return None
                    
            except requests.Timeout:
                logger.warning(f"Timeout on attempt {attempt}/{self.max_retries}")
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)
                continue
                
            except Exception as e:
                logger.error(f"Search failed: {str(e)}")
                return None
        
        logger.warning(f"Search failed after {self.max_retries} attempts: {query}")
        return None
    
    def _get_priority_domains(self, brand: str) -> list:
        """
        Generate prioritized domain list for search.
        
        Args:
            brand: Brand name
            
        Returns:
            List of domains to prioritize in search
        """
        # Clean brand name for domain generation
        brand_domain = brand.lower().replace(' ', '').replace('-', '')
        
        # Format domain templates
        domains = [d.format(brand_domain=brand_domain) for d in DOMAIN_PRIORITY]
        
        return domains
    
    def _validate_url(self, url: str) -> bool:
        """
        Validate URL format.
        
        Args:
            url: URL to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not url or not isinstance(url, str):
            return False
        
        if not url.startswith(('http://', 'https://')):
            return False
        
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
    
    def _generate_cache_key(self, brand: str, product: str) -> str:
        """
        Generate consistent cache key.
        
        Args:
            brand: Brand name
            product: Product name
            
        Returns:
            MD5 hash of brand|product
        """
        key = f"{brand.lower()}|{product.lower()}"
        return hashlib.md5(key.encode()).hexdigest()
    
    def _load_cache(self) -> Dict:
        """Load cache from file"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                    logger.debug(f"Loaded {len(cache)} cached URLs")
                    return cache
            except Exception as e:
                logger.error(f"Cache load failed: {e}")
                return {}
        return {}
    
    def _save_cache(self):
        """Save cache to file"""
        try:
            # Atomic write using temp file
            temp_file = self.cache_file.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
            temp_file.replace(self.cache_file)
            logger.debug(f"Saved {len(self.cache)} URLs to cache")
        except Exception as e:
            logger.error(f"Cache save failed: {e}")
