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
        
        logger.info(f"ClaudeEnricher initialized with model: {self.model}")
    
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
                    message = self.client.messages.create(
                        model=self.model,
                        max_tokens=self.max_tokens['variants'],
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
