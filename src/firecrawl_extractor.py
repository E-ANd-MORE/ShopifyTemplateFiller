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
                    # Parse HTML for image URLs with more context
                    import re
                    # Find img tags with full attributes (src, alt, class, width, height)
                    img_full_pattern = r'<img([^>]+)>'
                    img_tags = re.findall(img_full_pattern, html_content, re.IGNORECASE)
                    
                    raw_images = []
                    for img_attrs in img_tags:
                        # Extract src (or data-src)
                        src_match = re.search(r'(?:src|data-src)=["\']([^"\']+)["\']', img_attrs, re.IGNORECASE)
                        if not src_match:
                            continue
                        
                        src = src_match.group(1)
                        
                        # Extract alt text
                        alt_match = re.search(r'alt=["\']([^"\']*)["\']', img_attrs, re.IGNORECASE)
                        alt = alt_match.group(1) if alt_match else ''
                        
                        # Extract class
                        class_match = re.search(r'class=["\']([^"\']*)["\']', img_attrs, re.IGNORECASE)
                        img_class = class_match.group(1).lower() if class_match else ''
                        
                        # Extract width/height if present
                        width_match = re.search(r'width=["\']?(\d+)', img_attrs, re.IGNORECASE)
                        height_match = re.search(r'height=["\']?(\d+)', img_attrs, re.IGNORECASE)
                        width = int(width_match.group(1)) if width_match else 0
                        height = int(height_match.group(1)) if height_match else 0
                        
                        raw_images.append({
                            'src': src,
                            'alt': alt,
                            'class': img_class,
                            'width': width,
                            'height': height
                        })
                    
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
        Filter and score images by relevance, prioritizing main images over thumbnails.
        
        Returns top 3 product images.
        
        Args:
            images: List of image dicts with src, alt, class, width, height
            product_name: Product name for relevance scoring
            
        Returns:
            List of top 3 image URLs (main product images, not thumbnails)
        """
        scored_images = []
        product_keywords = product_name.lower().split()
        
        # Skip patterns for non-product images
        skip_patterns = [
            'logo', 'icon', 'badge', 'button',
            'arrow', 'star', 'rating', 'banner',
            'header', 'footer', 'nav', 'menu',
            'social', 'facebook', 'twitter', 'instagram',
            'checkout', 'cart', 'search', 'sprite',
            'warranty', 'insurance', 'bullet-point',
            'guarantee', 'shipping', 'return', 'policy'
        ]
        
        # Thumbnail indicators (NEGATIVE scoring)
        thumbnail_patterns = [
            'thumb', 'thumbnail', 'small', 'mini', 'tiny',
            '_s.', '_xs.', '_sm.', '-thumb', '-small',
            '/thumbs/', '/thumbnails/', '/small/',
            'icon', 'preview', 'swatch'
        ]
        
        # Main image indicators (POSITIVE scoring)
        main_image_patterns = [
            'large', 'main', 'primary', 'hero', 'zoom',
            '_l.', '_xl.', '_lg.', '-large', '-main',
            '/large/', '/original/', '/full/',
            'product-image', 'product_image', 'detail'
        ]
        
        for img in images:
            src = img.get('src', '').strip()
            alt = img.get('alt', '').lower()
            img_class = img.get('class', '').lower()
            width = img.get('width', 0)
            height = img.get('height', 0)
            
            # Must be HTTPS
            if not src.startswith('https://'):
                logger.debug(f"Skipping non-HTTPS image: {src[:50]}")
                continue
            
            src_lower = src.lower()
            
            # Skip common non-product images
            if any(pattern in src_lower for pattern in skip_patterns):
                logger.debug(f"Skipping non-product image: {src[:50]}")
                continue
            
            if any(pattern in alt for pattern in skip_patterns):
                logger.debug(f"Skipping by alt text: {alt[:50]}")
                continue
            
            if any(pattern in img_class for pattern in skip_patterns):
                logger.debug(f"Skipping by class: {img_class[:50]}")
                continue
            
            # Skip tracking pixels and tiny images
            if src.endswith(('gif', '1x1', 'pixel')):
                continue
            
            # Skip data URIs
            if src.startswith('data:'):
                continue
            
            # CRITICAL: Skip thumbnails (negative scoring)
            is_thumbnail = any(pattern in src_lower for pattern in thumbnail_patterns)
            if is_thumbnail:
                logger.debug(f"Skipping thumbnail: {src[:70]}")
                continue
            
            # Check class for thumbnail indicators
            if any(pattern in img_class for pattern in thumbnail_patterns):
                logger.debug(f"Skipping thumbnail by class: {img_class}")
                continue
            
            # Calculate relevance score
            score = 0
            
            # Image size (CRITICAL: bigger = better, max 50 points)
            if width > 0 and height > 0:
                # Large images (800+ px) get high score
                if width >= 800 or height >= 800:
                    score += 50
                elif width >= 500 or height >= 500:
                    score += 35
                elif width >= 300 or height >= 300:
                    score += 20
                elif width >= 150 or height >= 150:
                    score += 5
                else:
                    # Very small images likely thumbnails
                    score -= 20
            
            # Main image indicators in URL (30 points)
            if any(pattern in src_lower for pattern in main_image_patterns):
                score += 30
                logger.debug(f"Main image detected: {src[:70]}")
            
            # Main image indicators in class (20 points)
            if any(pattern in img_class for pattern in main_image_patterns):
                score += 20
            
            # Alt text relevance (20 points)
            if alt:
                keyword_matches = sum(1 for kw in product_keywords if kw in alt)
                score += keyword_matches * 5
                
                # Bonus for "product" in alt
                if 'product' in alt:
                    score += 5
            
            # Image format preference (10 points)
            if src_lower.endswith('.jpg') or src_lower.endswith('.jpeg'):
                score += 10
            elif src_lower.endswith('.png'):
                score += 8
            elif src_lower.endswith('.webp'):
                score += 6
            
            # CDN indicators (10 points)
            cdn_keywords = ['cdn', 'images', 'assets', 's3', 'cloudfront', 'media']
            if any(cdn in src_lower for cdn in cdn_keywords):
                score += 10
            
            # Product-specific keywords in URL (15 points)
            if 'product' in src_lower:
                score += 15
            
            scored_images.append({
                'url': src,
                'score': score,
                'alt': alt[:50] if alt else '',
                'width': width,
                'height': height
            })
        
        # Sort by score (highest first) and return top 3
        scored_images.sort(key=lambda x: x['score'], reverse=True)
        
        # Log top candidates for debugging
        if scored_images:
            logger.debug("Top image candidates:")
            for i, img in enumerate(scored_images[:5]):
                logger.debug(f"  {i+1}. Score: {img['score']}, Size: {img['width']}x{img['height']}, URL: {img['url'][:80]}")
        
        # Get top 3 URLs
        result_urls = [img['url'] for img in scored_images[:3]]
        
        # Upgrade to full-size versions (e.g., Amazon, Shopify CDNs)
        result = [self._upgrade_to_fullsize(url) for url in result_urls]
        
        logger.debug(f"Filtered {len(images)} images to {len(result)} main product images")
        
        return result
    
    def _upgrade_to_fullsize(self, url: str) -> str:
        """
        Upgrade image URLs to their full-size versions.
        
        Handles:
        - Amazon images: Remove size constraints (_AC_UL*, _SR*, etc.)
        - Shopify CDN: Remove size parameters
        - Other CDNs: Remove common thumbnail indicators
        
        Args:
            url: Original image URL
            
        Returns:
            Full-size image URL
        """
        import re
        
        original_url = url
        
        # Amazon images: Remove size constraints
        # Example: https://images-na.ssl-images-amazon.com/images/I/51IGO6BIBeL._AC_UL116_SR116,116_.jpg
        # Result:  https://images-na.ssl-images-amazon.com/images/I/51IGO6BIBeL.jpg
        if 'amazon.com' in url or 'ssl-images-amazon' in url:
            # Remove Amazon's size modifiers (_AC_*, _SR*, _UL*, _SL*, _UX*, _UY*)
            url = re.sub(r'\._AC_[A-Z0-9,_]+', '', url)
            url = re.sub(r'\._SR[0-9,_]+', '', url)
            url = re.sub(r'\._UL[0-9,_]+', '', url)
            url = re.sub(r'\._SL[0-9,_]+', '', url)
            url = re.sub(r'\._UX[0-9,_]+', '', url)
            url = re.sub(r'\._UY[0-9,_]+', '', url)
            url = re.sub(r'\._UF[0-9,_]+', '', url)
            
            # Ensure proper extension
            if not url.endswith(('.jpg', '.jpeg', '.png', '.webp')):
                url += '.jpg'
        
        # Shopify CDN: Remove size parameters
        # Example: https://cdn.shopify.com/s/files/1/0123/4567/products/image_200x200.jpg
        # Result:  https://cdn.shopify.com/s/files/1/0123/4567/products/image.jpg
        elif 'shopify.com' in url or 'cdn.shopify' in url:
            url = re.sub(r'_\d+x\d+(@\dx)?\.', '.', url)
            url = re.sub(r'_small\.', '.', url)
            url = re.sub(r'_medium\.', '.', url)
            url = re.sub(r'_thumb\.', '.', url)
        
        # Generic: Remove common thumbnail size patterns
        else:
            # Remove _200x200, _thumb, _small patterns
            url = re.sub(r'[-_](thumb|small|thumbnail|mini|tiny|icon|preview)[-_\.]', '.', url)
            url = re.sub(r'[-_]\d+x\d+[-_\.]', '.', url)
        
        if url != original_url:
            logger.debug(f"Upgraded image URL:")
            logger.debug(f"  Before: {original_url[:100]}")
            logger.debug(f"  After:  {url[:100]}")
        
        return url
    
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
