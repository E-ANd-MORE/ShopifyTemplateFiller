"""
Module 4: Claude Enricher
Enrich product data using Claude AI (Anthropic API).
"""
import logging
import json
import re
import hashlib
import time
from typing import List, Dict, Optional, Any
from pathlib import Path

from anthropic import Anthropic, RateLimitError

from config import ANTHROPIC_API_KEY, API_CONFIG, CACHE_DIR, SHOPIFY_CATEGORIES
from src.models import ProductData

logger = logging.getLogger(__name__)


class ClaudeEnricher:
    """
    Enrich product data using Claude AI.
    
    Operations:
    1. Extract variant attributes from product names (not create new)
    2. Generate product descriptions
    3. Assign categories
    4. Generate SEO tags
    
    All responses are cached to reduce API costs.
    """
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or ANTHROPIC_API_KEY
        if not self.api_key:
            raise ValueError("Anthropic API key is required")
        
        self.client = Anthropic(api_key=self.api_key)
        self.config = API_CONFIG['claude']
        self.model = self.config['model']
        self.temperature = self.config['temperature']
        self.max_tokens = self.config['max_tokens']
        
        # Cache setup
        self.cache_file = Path(CACHE_DIR) / 'claude_cache.json'
        self.cache = self._load_cache()
        
        # Rate limiting setup
        self.rate_config = self.config.get('rate_limit', {})
        self.requests_per_minute = self.rate_config.get('requests_per_minute', 50)
        self.adaptive_delay = self.rate_config.get('adaptive_delay', True)
        self.min_delay = self.rate_config.get('min_delay', 0.1)
        self.max_delay = self.rate_config.get('max_delay', 2.0)
        
        # Track recent requests for rate limiting
        from collections import deque
        self.request_times = deque(maxlen=self.requests_per_minute)
        self.consecutive_rate_limits = 0
        
        logger.info(f"ClaudeEnricher initialized with model: {self.model}")
        logger.info(f"Rate limiting: {self.requests_per_minute} req/min, adaptive={self.adaptive_delay}")
    
    def _adaptive_rate_limit(self):
        """
        Smart rate limiting that adapts based on API responses.
        Prevents 429 errors while maximizing throughput.
        """
        current_time = time.time()
        
        # Remove requests older than 60 seconds
        while self.request_times and current_time - self.request_times[0] > 60:
            self.request_times.popleft()
        
        # Check if we're approaching rate limit
        if len(self.request_times) >= self.requests_per_minute * 0.9:  # 90% threshold
            # Calculate time to wait until oldest request expires
            if self.request_times:
                oldest_request = self.request_times[0]
                wait_time = 60 - (current_time - oldest_request)
                if wait_time > 0:
                    logger.debug(f"Rate limit approaching, waiting {wait_time:.2f}s")
                    time.sleep(wait_time + 0.1)  # Small buffer
                    return
        
        # Adaptive delay based on recent rate limit hits
        if self.adaptive_delay and self.consecutive_rate_limits > 0:
            # Increase delay exponentially with consecutive rate limits
            delay = min(self.min_delay * (2 ** self.consecutive_rate_limits), self.max_delay)
            logger.debug(f"Adaptive delay: {delay:.2f}s (rate limits: {self.consecutive_rate_limits})")
            time.sleep(delay)
        else:
            # Minimum delay between requests
            time.sleep(self.min_delay)
        
        # Track this request
        self.request_times.append(current_time)
    
    def _handle_rate_limit_success(self):
        """Reset rate limit counter on successful request"""
        if self.consecutive_rate_limits > 0:
            logger.debug(f"Request successful, resetting rate limit counter")
            self.consecutive_rate_limits = 0
    
    def _handle_rate_limit_error(self):
        """Increment rate limit counter on 429 error"""
        self.consecutive_rate_limits += 1
        logger.warning(f"Rate limit hit (consecutive: {self.consecutive_rate_limits})")
    
    def enrich_product_batch(self, brand: str, product_name: str, price: float, category: str = None) -> Dict[str, Any]:
        """
        OPTIMIZED: Batch all enrichment tasks into a single Claude API call.
        This reduces 10 API calls to 1, making processing 10x faster!
        
        Args:
            brand: Brand name
            product_name: Product name
            price: Product price
            category: Pre-assigned category (optional)
            
        Returns:
            Dict containing all enriched fields
        """
        cache_key = f"batch|{brand}|{product_name}|{price}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        prompt = f"""You are a product content expert. Generate ALL the following for this product in a single JSON response:

**PRODUCT INFO:**
Brand: {brand}
Product: {product_name}
Price: ${price:.2f}

**GENERATE THE FOLLOWING (return as JSON):**

1. **cleaned_name**: Clean product name (remove special chars, make readable, 5-7 words max)

2. **description**: Professional HTML product description (2-3 sentences, highlight benefits, SEO-friendly, no markdown)

3. **category**: Product category in format "Main Category > Subcategory" (e.g., "Health & Beauty > Hair Care", "Health & Beauty > Skincare", "Health & Beauty > Oral Care")

4. **tags**: Array of 6-10 SEO tags (lowercase, hyphenated)

5. **benefits**: Product benefits (3-5 points, like Goli format with line breaks)

6. **ingredients**: Ingredient qualities description (vegan, organic, etc.)

7. **good_for**: Social/environmental responsibility statement (1-2 sentences)

8. **suggested_usage**: Usage instructions with dosage and frequency

9. **allergy_info**: Warnings/disclaimers appropriate for product type

Return ONLY valid JSON in this exact format:
{{
  "cleaned_name": "...",
  "description": "...",
  "category": "Main Category > Subcategory",
  "tags": ["tag1", "tag2", ...],
  "benefits": "...",
  "ingredients": "...",
  "good_for": "...",
  "suggested_usage": "...",
  "allergy_info": "..."
}}"""

        try:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Apply adaptive rate limiting before request
                    self._adaptive_rate_limit()
                    
                    message = self.client.messages.create(
                        model=self.model,
                        max_tokens=2000,  # Larger for batched response
                        temperature=self.temperature,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    
                    # Success - reset rate limit counter
                    self._handle_rate_limit_success()
                    break
                except RateLimitError:
                    self._handle_rate_limit_error()
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 3
                        logger.warning(f"Rate limit hit, waiting {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        raise
            
            response_text = message.content[0].text.strip()
            
            # Parse JSON response
            result = self._parse_json_response(response_text, default={})
            
            # Validate and set defaults
            enriched = {
                "cleaned_name": result.get("cleaned_name", product_name),
                "description": result.get("description", f"<p>Premium <strong>{brand}</strong> product. {product_name}.</p>"),
                "category": result.get("category", "Health & Beauty > Other"),  # Get from Claude for Type field
                "tags": result.get("tags", [brand.lower(), "product", "beauty"]),
                "benefits": result.get("benefits", ""),
                "ingredients": result.get("ingredients", ""),
                "good_for": result.get("good_for", ""),
                "suggested_usage": result.get("suggested_usage", ""),
                "allergy_info": result.get("allergy_info", "")
            }
            
            # Clean markdown artifacts
            for key in ["description", "benefits", "ingredients", "good_for", "suggested_usage", "allergy_info"]:
                if isinstance(enriched[key], str):
                    enriched[key] = enriched[key].replace('```', '').replace('**', '').replace('*', '').replace('`', '')
            
            self.cache[cache_key] = enriched
            self._save_cache()
            
            logger.debug(f"Batch enriched: {enriched['cleaned_name']}")
            return enriched
            
        except Exception as e:
            logger.error(f"Batch enrichment failed: {str(e)}")
            # Return fallback values
            return {
                "cleaned_name": product_name,
                "description": f"<p>Premium <strong>{brand}</strong> product. {product_name}.</p>",
                "category": "Health & Beauty > Other",  # Default fallback
                "tags": [brand.lower(), "product", "beauty"],
                "benefits": "",
                "ingredients": "",
                "good_for": "",
                "suggested_usage": "",
                "allergy_info": ""
            }
    
    def extract_variants(self, product_name: str) -> List[Dict[str, str]]:
        """
        Extract variant attributes FROM the product name only.
        
        CRITICAL: Does NOT create new variants. Only identifies what
        variant attributes exist in the given product name.
        
        Args:
            product_name: Product name to analyze
            
        Returns:
            List of variant dicts: [{"name": "Color", "value": "Black"}, ...]
            Empty list if no variants found or on error
        """
        cache_key = f"variants|{product_name}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        prompt = f"""Extract ALL product variant attributes from this product name ONLY.

Product Name: {product_name}

Find any of these variant types that exist in the name:
- Color/Shade (e.g., Black, Blue, Red, Pink, Nude)
- Size/Volume (e.g., 50ml, 100g, L, XL)
- Flavor/Scent (e.g., Mint, Rose, Vanilla)
- Type/Formula (e.g., Ammonia-Free, Organic, Matte)
- Strength/Level (e.g., Light, Medium, Heavy)
- Gender/Age (e.g., Men, Women, Unisex)
- Finish (e.g., Glossy, Matte, Shimmer)

Return ONLY valid JSON array. Example:
[{{"name": "Color", "value": "Black"}}, {{"name": "Size", "value": "50ml"}}]

If no variants are found in the name, return: []

Important: Extract ONLY what exists in the product name. Do NOT invent variants."""

        try:
            # Retry logic for rate limiting
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Apply adaptive rate limiting before request
                    self._adaptive_rate_limit()
                    
                    message = self.client.messages.create(
                        model=self.model,
                        max_tokens=self.max_tokens['variants'],
                        temperature=self.temperature,
                        messages=[{
                            "role": "user",
                            "content": prompt
                        }]
                    )
                    
                    # Success - reset rate limit counter
                    self._handle_rate_limit_success()
                    break
                except RateLimitError as e:
                    self._handle_rate_limit_error()
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 2  # Exponential backoff: 2s, 4s, 8s
                        logger.warning(f"Rate limit hit, waiting {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        raise
            
            response_text = message.content[0].text.strip()
            
            # Parse JSON response
            variants = self._parse_json_response(response_text, default=[])
            
            # Validate variants
            variants = [
                v for v in variants
                if isinstance(v, dict) and 'name' in v and 'value' in v
                and isinstance(v['name'], str) and isinstance(v['value'], str)
            ]
            
            self.cache[cache_key] = variants
            self._save_cache()
            
            logger.debug(f"Extracted {len(variants)} variants from: {product_name}")
            return variants
            
        except Exception as e:
            logger.error(f"Variant extraction failed: {str(e)}")
            return []
    
    def generate_description(self, brand: str, product_name: str, price: float) -> str:
        """
        Generate professional product description.
        
        Args:
            brand: Brand name
            product_name: Product name
            price: Product price
            
        Returns:
            HTML-formatted description (2-3 sentences)
            Fallback description on error
        """
        cache_key = f"description|{brand}|{product_name}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        prompt = f"""Create a professional product description for e-commerce.

Brand: {brand}
Product: {product_name}
Price: ${price:.2f}

Requirements:
- 2-3 sentences only
- Highlight key benefits and features
- Professional, appealing tone
- SEO-friendly
- Use basic HTML if beneficial (<p>, <strong>, <em>)
- NO markdown formatting
- NO asterisks or backticks
- NO promotional language like "Buy Now"

Return ONLY the description text, nothing else."""

        try:
            # Retry logic for rate limiting
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    message = self.client.messages.create(
                        model=self.model,
                        max_tokens=self.max_tokens['description'],
                        temperature=self.temperature,
                        messages=[{
                            "role": "user",
                            "content": prompt
                        }]
                    )
                    break
                except RateLimitError as e:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 2  # Exponential backoff: 2s, 4s, 8s
                        logger.warning(f"Rate limit hit, waiting {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        raise
            
            description = message.content[0].text.strip()
            
            # Clean markdown artifacts if present
            description = description.replace('```', '')
            description = description.replace('**', '')
            description = description.replace('*', '')
            description = description.replace('`', '')
            
            # Ensure max length
            if len(description) > 500:
                description = description[:497] + '...'
            
            self.cache[cache_key] = description
            self._save_cache()
            
            logger.debug(f"Generated description for: {product_name}")
            return description
            
        except Exception as e:
            logger.error(f"Description generation failed: {str(e)}")
            fallback = f"<p>Premium <strong>{brand}</strong> product. {product_name}.</p>"
            return fallback
    
    def assign_category(self, brand: str, product_name: str) -> str:
        """
        Assign product to Shopify Standard Product Taxonomy category dynamically.
        Uses keyword map as examples for Claude to understand patterns.
        
        Args:
            brand: Brand name
            product_name: Product name
            
        Returns:
            Valid Shopify category in hierarchical format (e.g., "Health & Beauty > Hair Care")
        """
        cache_key = f"category|{product_name}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Get keyword map for examples
        keyword_map = self._get_keyword_map()
        
        # Format examples for Claude
        examples = []
        for category, keywords in list(keyword_map.items())[:5]:  # Show 5 examples
            examples.append(f"- {category}: {', '.join(keywords[:3])}")
        examples_text = "\n".join(examples)
        
        prompt = f"""Assign this product to a Shopify Standard Product Taxonomy category.

Brand: {brand}
Product: {product_name}

Use the hierarchical format: "Top Level > Subcategory"

Example category patterns (for reference, you can create similar ones):
{examples_text}

Based on the product name and brand, create an appropriate category in the format:
"Health & Beauty > [Specific Category]"

For beauty/personal care products, use subcategories like:
- Hair Care (for shampoo, conditioner, hair products)
- Skin Care (for face creams, serums, moisturizers)
- Oral Care (for mouthwash, toothpaste, dental products)
- Bath & Body (for body wash, soap, lotions, feminine care)
- Makeup (for cosmetics, lipstick, foundation)
- Fragrance (for perfumes, colognes)
- Nail Care (for nail polish, manicure products)
- Personal Care (for vitamins, supplements, health products)

Return ONLY the category string in format: "Health & Beauty > [Subcategory]"
Nothing else."""

        try:
            # Retry logic for rate limiting
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    message = self.client.messages.create(
                        model=self.model,
                        max_tokens=self.max_tokens['category'],
                        temperature=self.temperature,
                        messages=[{
                            "role": "user",
                            "content": prompt
                        }]
                    )
                    break
                except RateLimitError as e:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 2  # Exponential backoff: 2s, 4s, 8s
                        logger.warning(f"Rate limit hit, waiting {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        raise
            
            response = message.content[0].text.strip()
            
            # Clean up response (remove quotes, extra whitespace)
            response = response.replace('"', '').replace("'", "").strip()
            
            # Validate format (should contain ">")
            if ">" in response and len(response) < 100:
                self.cache[cache_key] = response
                self._save_cache()
                logger.debug(f"Assigned category '{response}' to: {product_name}")
                return response
            
            # Fallback: use keyword-based category assignment
            fallback_cat = self._guess_category_from_keywords(product_name)
            logger.debug(f"Category fallback to '{fallback_cat}' for: {product_name}")
            return fallback_cat
            
        except Exception as e:
            logger.error(f"Category assignment failed: {str(e)}")
            return self._guess_category_from_keywords(product_name)
    
    def _get_keyword_map(self) -> Dict[str, List[str]]:
        """
        Get keyword map for category assignment.
        Used both as examples for Claude and for fallback keyword matching.
        
        Returns:
            Dict mapping categories to keyword lists
        """
        return {
            "Health & Beauty > Hair Care": [
                "shampoo", "conditioner", "hair", "scalp", "haircare"
            ],
            "Health & Beauty > Skin Care": [
                "face", "facial", "toner", "cleanser", "moisturizer", 
                "serum", "cream", "mask", "makeup remover", "skin"
            ],
            "Health & Beauty > Oral Care": [
                "mouthwash", "mouth wash", "toothpaste", "dental", 
                "oral", "teeth", "breath"
            ],
            "Health & Beauty > Bath & Body": [
                "body", "bath", "shower", "soap", "lotion", 
                "feminine", "intimate", "wipes", "body wash"
            ],
            "Health & Beauty > Makeup": [
                "lipstick", "foundation", "mascara", "eyeshadow",
                "makeup", "cosmetic", "powder", "blush"
            ],
            "Health & Beauty > Fragrance": [
                "perfume", "cologne", "fragrance", "eau de", "scent"
            ],
            "Health & Beauty > Nail Care": [
                "nail", "polish", "manicure", "pedicure"
            ],
            "Health & Beauty > Personal Care": [
                "vitamin", "supplement", "medicine", "health", "wellness"
            ],
        }
    
    def _guess_category_from_keywords(self, product_name: str) -> str:
        """
        Guess category based on product name keywords.
        Fallback method when Claude fails or returns invalid category.
        """
        name_lower = product_name.lower()
        
        # Get keyword map
        keyword_map = self._get_keyword_map()
        
        # Check keywords
        for category, keywords in keyword_map.items():
            for keyword in keywords:
                if keyword in name_lower:
                    return category
        
        # Default fallback - use most common category
        return "Health & Beauty > Skin Care"
    
    def clean_product_name(self, raw_name: str, brand: str) -> str:
        """
        Clean and normalize product name using Claude AI.
        
        Removes unnecessary characters, normalizes spacing, makes it human-readable.
        
        Args:
            raw_name: Raw product name from CSV
            brand: Brand name
            
        Returns:
            Clean, human-readable product name
        """
        cache_key = f"clean_name|{raw_name}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        prompt = f"""Clean this product name to make it human-readable and professional.

Brand: {brand}
Raw Name: {raw_name}

Requirements:
- Remove excessive capitalization
- Add proper spacing
- Remove special characters like XML, PC, etc. if not part of actual name
- Keep important details (size, color, type)
- Make it natural and readable
- Keep it concise (max 5-7 words)

Return ONLY the cleaned product name, nothing else.

Examples:
"BEAUTYSYSTEMMW-CAPSULESWITHFLAVORPIECESXML" → "MW Capsules with Flavor Pieces"
"BEAUTYSYSTEMFEMININEINTIMATE WIPESPCWITHALOEVERAVITAMINE" → "Feminine Intimate Wipes with Aloe Vera & Vitamin E"
"""

        try:
            # Retry logic for rate limiting
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    message = self.client.messages.create(
                        model=self.model,
                        max_tokens=self.max_tokens['clean_name'],
                        temperature=self.temperature,
                        messages=[{
                            "role": "user",
                            "content": prompt
                        }]
                    )
                    break
                except RateLimitError as e:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 2  # Exponential backoff: 2s, 4s, 8s
                        logger.warning(f"Rate limit hit, waiting {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        raise
            
            cleaned_name = message.content[0].text.strip()
            
            # Remove quotes if Claude added them
            cleaned_name = cleaned_name.strip('"\'')
            
            # Ensure not too long
            if len(cleaned_name) > 100:
                cleaned_name = cleaned_name[:97] + '...'
            
            # Cache and return
            self.cache[cache_key] = cleaned_name
            self._save_cache()
            
            logger.debug(f"Cleaned name: '{raw_name}' → '{cleaned_name}'")
            return cleaned_name
            
        except Exception as e:
            logger.error(f"Name cleaning failed: {str(e)}")
            # Fallback: basic cleaning
            fallback = raw_name.replace('BEAUTYSYSTEM', '').replace('XML', '').replace('PC', '')
            fallback = ' '.join(fallback.split())
            return fallback.title()
    
    def generate_tags(self, brand: str, product_name: str, category: str) -> List[str]:
        """
        Generate SEO-optimized tags.
        
        Args:
            brand: Brand name
            product_name: Product name
            category: Product category
            
        Returns:
            List of 6-10 lowercase, hyphenated tags
            Fallback tags on error
        """
        cache_key = f"tags|{brand}|{product_name}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        prompt = f"""Generate SEO tags for this product.

Brand: {brand}
Product: {product_name}
Category: {category}

Requirements:
- Generate 6-10 tags
- One tag per line
- Lowercase only
- Hyphenate multi-word tags (e.g., "hair-care")
- Focus on searchability and keywords
- Include: brand, category, product type, benefits, use-cases

Format (one per line):
tag1
tag2
tag3

Example for "Shampoo":
hair-care
shampoo
hair-treatment
beauty
haircare-product"""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens['tags'],
                temperature=self.temperature,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
            response = message.content[0].text.strip()
            
            # Parse tags (one per line)
            tags = [
                tag.strip().lower()
                for tag in response.split('\n')
                if tag.strip() and not tag.startswith('#')
            ]
            
            # Validate tags
            tags = [
                tag for tag in tags
                if tag and 2 <= len(tag) <= 50
                and all(c in 'abcdefghijklmnopqrstuvwxyz0123456789- ' for c in tag)
            ]
            
            # Remove duplicates while preserving order
            seen = set()
            unique_tags = []
            for tag in tags:
                if tag not in seen:
                    seen.add(tag)
                    unique_tags.append(tag)
            tags = unique_tags
            
            # Ensure 6-10 tags
            if len(tags) < 6:
                # Add fallback tags
                fallback = [brand.lower(), category.lower().replace(' ', '-'), 'product', 'beauty']
                tags.extend([t for t in fallback if t not in tags])
            
            tags = tags[:10]
            
            self.cache[cache_key] = tags
            self._save_cache()
            
            logger.debug(f"Generated {len(tags)} tags for: {product_name}")
            return tags
            
        except Exception as e:
            logger.error(f"Tag generation failed: {str(e)}")
            fallback = [
                brand.lower(),
                category.lower().replace(' ', '-'),
                'product',
                'beauty',
                'shop',
                'online'
            ]
            return fallback
    
    def generate_benefits(self, brand: str, product_name: str, category: str) -> str:
        """
        Generate product benefits content for metafield.
        
        Args:
            brand: Brand name
            product_name: Product name
            category: Product category
            
        Returns:
            Benefits text describing key product advantages
        """
        cache_key = f"benefits|{brand}|{product_name}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        prompt = f"""Generate product benefits for this item. Focus on key advantages and what makes it valuable.

Brand: {brand}
Product: {product_name}
Category: {category}

Create 3-5 bullet points describing the main benefits. Format like Goli example:
"Patented Formula, Essential Vitamins, Great Taste: Our patented formula contains essential Vitamin B12 to help support cellular energy production, immune function, heart health, healthy nutrient metabolism, a healthy nervous system and overall health and wellbeing. 

Apple Cider Vinegar has traditionally been used for digestion and gut health. Our unique flavor profile combined with essential vitamins makes Goli® ACV Gummies a delicious addition to your daily health routine."

Keep it professional, benefit-focused, and informative. Separate benefits with line breaks.
Return ONLY the benefits text, nothing else."""

        try:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    message = self.client.messages.create(
                        model=self.model,
                        max_tokens=400,
                        temperature=self.temperature,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    break
                except RateLimitError:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 2
                        logger.warning(f"Rate limit hit, waiting {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        raise
            
            benefits = message.content[0].text.strip()
            benefits = benefits.replace('```', '').replace('**', '').replace('*', '')
            
            self.cache[cache_key] = benefits
            self._save_cache()
            return benefits
            
        except Exception as e:
            logger.error(f"Benefits generation failed: {str(e)}")
            return ""
    
    def generate_ingredients(self, brand: str, product_name: str, category: str) -> str:
        """
        Generate custom ingredients information.
        
        Args:
            brand: Brand name
            product_name: Product name
            category: Product category
            
        Returns:
            Ingredients description
        """
        cache_key = f"ingredients|{brand}|{product_name}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        prompt = f"""Generate ingredient information for this product. Focus on key ingredients and their qualities.

Brand: {brand}
Product: {product_name}
Category: {category}

Format like Goli example:
"Vegan, Non-GMO, Gluten-free & Gelatin-free: Each bottle of Goli® ACV Gummies contains 60 delicious, vegan, non-gmo, gluten-free & gelatin-free gummies, which makes them suitable for almost any lifestyle."

Be specific about ingredient qualities (natural, organic, vegan, cruelty-free, etc.).
Return ONLY the ingredients text, nothing else."""

        try:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    message = self.client.messages.create(
                        model=self.model,
                        max_tokens=300,
                        temperature=self.temperature,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    break
                except RateLimitError:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 2
                        logger.warning(f"Rate limit hit, waiting {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        raise
            
            ingredients = message.content[0].text.strip()
            ingredients = ingredients.replace('```', '').replace('**', '').replace('*', '')
            
            self.cache[cache_key] = ingredients
            self._save_cache()
            return ingredients
            
        except Exception as e:
            logger.error(f"Ingredients generation failed: {str(e)}")
            return ""
    
    def generate_good_for(self, brand: str, product_name: str, category: str) -> str:
        """
        Generate 'Good For' content describing social/environmental responsibility.
        
        Args:
            brand: Brand name
            product_name: Product name
            category: Product category
            
        Returns:
            Good For text
        """
        cache_key = f"goodfor|{brand}|{product_name}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        prompt = f"""Generate 'Good For' content describing positive social or environmental impact.

Brand: {brand}
Product: {product_name}
Category: {category}

Format like Goli example:
"Goli for Good: Goli is a proud supporter of Vitamin Angels and Eden Reforestation Projects."

Focus on: sustainability, charitable giving, environmental initiatives, social responsibility.
Keep it brief (1-2 sentences).
Return ONLY the text, nothing else."""

        try:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    message = self.client.messages.create(
                        model=self.model,
                        max_tokens=200,
                        temperature=self.temperature,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    break
                except RateLimitError:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 2
                        logger.warning(f"Rate limit hit, waiting {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        raise
            
            good_for = message.content[0].text.strip()
            good_for = good_for.replace('```', '').replace('**', '').replace('*', '')
            
            self.cache[cache_key] = good_for
            self._save_cache()
            return good_for
            
        except Exception as e:
            logger.error(f"Good For generation failed: {str(e)}")
            return ""
    
    def generate_suggested_usage(self, brand: str, product_name: str, category: str) -> str:
        """
        Generate suggested usage instructions.
        
        Args:
            brand: Brand name
            product_name: Product name
            category: Product category
            
        Returns:
            Usage instructions
        """
        cache_key = f"usage|{brand}|{product_name}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        prompt = f"""Generate suggested usage instructions for this product.

Brand: {brand}
Product: {product_name}
Category: {category}

Format like Goli example:
"Dosage:
2 Gummies

Instructions:
Take 1-2 gummies, 3 times daily. Chew thoroughly."

Be specific about dosage, frequency, and any special instructions.
Return ONLY the usage text, nothing else."""

        try:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    message = self.client.messages.create(
                        model=self.model,
                        max_tokens=250,
                        temperature=self.temperature,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    break
                except RateLimitError:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 2
                        logger.warning(f"Rate limit hit, waiting {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        raise
            
            usage = message.content[0].text.strip()
            usage = usage.replace('```', '').replace('**', '').replace('*', '')
            
            self.cache[cache_key] = usage
            self._save_cache()
            return usage
            
        except Exception as e:
            logger.error(f"Usage generation failed: {str(e)}")
            return ""
    
    def generate_allergy_info(self, brand: str, product_name: str, category: str) -> str:
        """
        Generate allergy information/warnings.
        
        Args:
            brand: Brand name
            product_name: Product name
            category: Product category
            
        Returns:
            Allergy information text
        """
        cache_key = f"allergy|{brand}|{product_name}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        prompt = f"""Generate allergy information or disclaimer for this product.

Brand: {brand}
Product: {product_name}
Category: {category}

Format like standard disclaimers:
"These statements have not been evaluated by the Food and Drug Administration. This product is not intended to diagnose, treat, cure or prevent any disease."

Or for cosmetics/topical products:
"For external use only. Avoid contact with eyes. Discontinue use if irritation occurs."

Include relevant warnings based on product category.
Return ONLY the allergy/warning text, nothing else."""

        try:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    message = self.client.messages.create(
                        model=self.model,
                        max_tokens=200,
                        temperature=self.temperature,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    break
                except RateLimitError:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 2
                        logger.warning(f"Rate limit hit, waiting {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        raise
            
            allergy = message.content[0].text.strip()
            allergy = allergy.replace('```', '').replace('**', '').replace('*', '')
            
            self.cache[cache_key] = allergy
            self._save_cache()
            return allergy
            
        except Exception as e:
            logger.error(f"Allergy info generation failed: {str(e)}")
            return ""
    
    def _parse_json_response(self, response: str, default: Any = None) -> Any:
        """
        Safely parse JSON from Claude response.
        
        Handles cases where Claude wraps response in markdown code blocks.
        """
        try:
            # Try direct parse first
            return json.loads(response)
        except json.JSONDecodeError:
            # Try extracting JSON from markdown
            json_match = re.search(r'(\[.*\]|\{.*\})', response, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group(1))
                except Exception:
                    pass
            
            logger.debug(f"Failed to parse JSON: {response[:100]}")
            return default
    
    def _load_cache(self) -> Dict:
        """Load cache from file"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                    logger.debug(f"Loaded {len(cache)} cached Claude responses")
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
            logger.debug(f"Saved {len(self.cache)} Claude responses to cache")
        except Exception as e:
            logger.error(f"Cache save failed: {e}")
