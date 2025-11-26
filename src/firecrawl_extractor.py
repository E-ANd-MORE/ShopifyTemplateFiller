"""
Module 3: Firecrawl Extractor
Extract product images from URLs using Firecrawl API.
"""
import logging
import time
import json
import requests
from typing import List, Dict, Optional
from pathlib import Path

from config import FIRECRAWL_API_KEY, API_CONFIG, CACHE_DIR

logger = logging.getLogger(__name__)


class FirecrawlExtractor:
    """
    Extract product images from URLs using Firecrawl API.
    
    Features:
    - Image filtering by relevance
    - Quality checks (HTTPS, format, size)
    - Top 3 images selection
    - Caching
    - Rate limiting
    """
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or FIRECRAWL_API_KEY
        if not self.api_key:
            raise ValueError("Firecrawl API key is required")
        
        self.config = API_CONFIG['firecrawl']
        self.endpoint = self.config['endpoint']
        self.timeout = self.config['timeout']
        self.max_retries = self.config['max_retries']
        self.rate_limit_delay = self.config['rate_limit_delay']
        
        # Cache setup
        self.cache_file = Path(CACHE_DIR) / 'firecrawl_cache.json'
        self.cache = self._load_cache()
        
        logger.info("FirecrawlExtractor initialized")
    
    def extract_images(self, url: str, product_name: str) -> List[str]:
        """
        Extract images from product URL.
        
        Args:
            url: Product page URL
            product_name: Product name for relevance filtering
            
        Returns:
            List of valid HTTPS image URLs (max 3), empty list on failure
        """
        # Input validation
        if not url or not url.startswith(('http://', 'https://')):
            logger.warning(f"Invalid URL: {url}")
            return []
        
        # Check cache
        cached_images = self.cache.get(url)
        if cached_images is not None:
            logger.debug(f"Cache hit for images from: {url}")
            return cached_images
        
        logger.info(f"Extracting images from: {url}")
        
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            # Updated payload for Firecrawl v1 API
            payload = {
                "url": url,
                "formats": ["html"],
                "onlyMainContent": True,
                "includeTags": ["img"],
                "waitFor": 2000  # Wait for images to load
            }
            
            response = requests.post(
                self.endpoint,
                json=payload,
                headers=headers,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                
                logger.debug(f"Firecrawl response keys: {list(data.keys())}")
                
                if not data.get('success', False):
                    logger.warning(f"Firecrawl reported failure for: {url}")
                    logger.debug(f"Firecrawl response: {data}")
                    self.cache[url] = []
                    self._save_cache()
                    return []
                
                # Extract images from HTML content
                raw_images = []
                
                # Get HTML content
                html_content = ''
                if 'data' in data and isinstance(data['data'], dict):
                    html_content = data['data'].get('html', '')
                
                if html_content:
                    # Parse HTML for image URLs
                    import re
                    # Find all img src attributes
                    img_pattern = r'<img[^>]+src=["\']([^"\']+)["\']'
                    img_urls = re.findall(img_pattern, html_content, re.IGNORECASE)
                    
                    # Also look for data-src (lazy loaded images)
                    data_src_pattern = r'<img[^>]+data-src=["\']([^"\']+)["\']'
                    data_src_urls = re.findall(data_src_pattern, html_content, re.IGNORECASE)
                    
                    all_urls = img_urls + data_src_urls
                    
                    # Convert to dict format with src key
                    raw_images = [{'src': url, 'alt': ''} for url in all_urls if url]
                    logger.debug(f"Extracted {len(raw_images)} image URLs from HTML")
                
                if not raw_images:
                    logger.debug(f"No images found on: {url}")
                    self.cache[url] = []
                    self._save_cache()
                    return []
                
                # Filter and score images
                filtered_images = self._filter_images(raw_images, product_name)
                
                # Cache and return
                self.cache[url] = filtered_images
                self._save_cache()
                
                logger.info(f"✓ Extracted {len(filtered_images)} images")
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
                
                return filtered_images
            
            elif response.status_code == 429:
                logger.warning(f"Rate limited by Firecrawl")
                time.sleep(5)
                return []
            
            else:
                logger.error(f"Firecrawl error {response.status_code}: {response.text[:200]}")
                return []
                
        except requests.Timeout:
            logger.error(f"Timeout extracting images from: {url}")
            return []
            
        except Exception as e:
            logger.error(f"Image extraction failed: {str(e)}")
            return []
    
    def _filter_images(self, images: List[Dict], product_name: str) -> List[str]:
        """
        Filter and score images by relevance.
        
        Returns top 3 product images.
        
        Args:
            images: List of image dicts from Firecrawl
            product_name: Product name for relevance scoring
            
        Returns:
            List of top 3 image URLs
        """
        scored_images = []
        product_keywords = product_name.lower().split()
        
        # Skip patterns for non-product images
        skip_patterns = [
            'logo', 'icon', 'badge', 'button',
            'arrow', 'star', 'rating', 'banner',
            'header', 'footer', 'nav', 'menu',
            'social', 'facebook', 'twitter', 'instagram',
            'checkout', 'cart', 'search'
        ]
        
        for img in images:
            src = img.get('src', '').strip()
            alt = img.get('alt', '').lower()
            
            # Must be HTTPS
            if not src.startswith('https://'):
                logger.debug(f"Skipping non-HTTPS image: {src[:50]}")
                continue
            
            # Skip common non-product images
            if any(pattern in src.lower() for pattern in skip_patterns):
                logger.debug(f"Skipping non-product image: {src[:50]}")
                continue
            
            if any(pattern in alt for pattern in skip_patterns):
                logger.debug(f"Skipping by alt text: {alt[:50]}")
                continue
            
            # Skip tracking pixels and tiny images
            if src.endswith(('gif', 'webp', '1x1', 'pixel')):
                continue
            
            # Skip data URIs
            if src.startswith('data:'):
                continue
            
            # Calculate relevance score
            score = 0
            
            # Alt text relevance (weight: 50%)
            if alt:
                keyword_matches = sum(1 for kw in product_keywords if kw in alt)
                score += keyword_matches * 5
                
                # Bonus for "product" in alt
                if 'product' in alt:
                    score += 3
            
            # Image format preference (weight: 30%)
            src_lower = src.lower()
            if src_lower.endswith('.jpg') or src_lower.endswith('.jpeg'):
                score += 3
            elif src_lower.endswith('.png'):
                score += 2
            elif src_lower.endswith('.webp'):
                score += 1
            
            # CDN indicators (weight: 20%)
            cdn_keywords = ['cdn', 'images', 'assets', 's3', 'cloudfront', 'media']
            if any(cdn in src_lower for cdn in cdn_keywords):
                score += 2
            
            # Product-specific keywords in URL
            if 'product' in src_lower:
                score += 3
            
            scored_images.append({
                'url': src,
                'score': score,
                'alt': alt[:50] if alt else ''
            })
        
        # Sort by score and return top 3
        scored_images.sort(key=lambda x: x['score'], reverse=True)
        result = [img['url'] for img in scored_images[:3]]
        
        logger.debug(f"Filtered {len(images)} images to {len(result)} valid images")
        
        return result
    
    def _load_cache(self) -> Dict:
        """Load cache from file"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                    logger.debug(f"Loaded {len(cache)} cached image sets")
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
            logger.debug(f"Saved {len(self.cache)} image sets to cache")
        except Exception as e:
            logger.error(f"Cache save failed: {e}")
