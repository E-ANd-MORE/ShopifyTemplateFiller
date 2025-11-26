"""
Module 4: Claude Enricher
Enrich product data using Claude AI (Anthropic API).
"""
import logging
import json
import re
import hashlib
from typing import List, Dict, Optional, Any
from pathlib import Path

from anthropic import Anthropic

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
            message = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens['variants'],
                temperature=self.temperature,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
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
            message = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens['description'],
                temperature=self.temperature,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
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
        Assign product to one of the predefined Shopify categories.
        
        Args:
            brand: Brand name
            product_name: Product name
            
        Returns:
            One of: Hair Care, Skincare, Makeup, Bath & Body, 
                   Fragrance, Tools & Accessories, Other
        """
        cache_key = f"category|{product_name}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        categories_list = "\n".join([f"{i+1}. {cat}" for i, cat in enumerate(SHOPIFY_CATEGORIES)])
        
        prompt = f"""Assign this product to ONE category from the list.

Brand: {brand}
Product: {product_name}

Available categories:
{categories_list}

Return ONLY the category name (e.g., "Hair Care" or "Skincare").
Choose the most appropriate category. If unsure, use "Other"."""

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
            
            response = message.content[0].text.strip()
            
            # Extract category name
            for cat in SHOPIFY_CATEGORIES:
                if cat.lower() in response.lower():
                    self.cache[cache_key] = cat
                    self._save_cache()
                    logger.debug(f"Assigned category '{cat}' to: {product_name}")
                    return cat
            
            # Fallback
            logger.debug(f"Category fallback to 'Other' for: {product_name}")
            return "Other"
            
        except Exception as e:
            logger.error(f"Category assignment failed: {str(e)}")
            return "Other"
    
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
            message = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens['clean_name'],
                temperature=self.temperature,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
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
