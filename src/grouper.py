"""
Module: Product Grouper
Groups individual product variants (CSV rows) into product groups.
Products with similar names from the same brand are grouped together.
"""
import logging
import re
from typing import List, Dict
from difflib import SequenceMatcher

from src.models import ProductData, ProductGroup
from config import GROUPING_CONFIG

logger = logging.getLogger(__name__)


class ProductGrouper:
    """
    Group product variants into product groups.
    
    Each CSV row is a variant. This class identifies which variants
    belong to the same base product by analyzing brand and product name.
    
    Example:
    - Row 1: "Shampoo Black 50ml" 
    - Row 2: "Shampoo Red 50ml"
    - Row 3: "Shampoo Black 100ml"
    → These become 1 product with 3 variants
    """
    
    def __init__(self):
        self.similarity_threshold = GROUPING_CONFIG['similarity_threshold']
        self.group_by_brand = GROUPING_CONFIG['group_by_brand']
    
    def group_products(self, products: List[ProductData]) -> List[ProductGroup]:
        """
        Group products by brand and similar names.
        
        Args:
            products: List of ProductData objects (one per CSV row)
            
        Returns:
            List of ProductGroup objects
        """
        logger.info("\n" + "=" * 80)
        logger.info("GROUPING PRODUCTS INTO VARIANTS")
        logger.info("=" * 80)
        
        if not products:
            logger.warning("No products to group")
            return []
        
        # Group by brand first
        brand_groups = self._group_by_brand(products)
        
        # Within each brand, group by similar names
        all_groups = []
        for brand, brand_products in brand_groups.items():
            logger.info(f"\nProcessing brand: {brand} ({len(brand_products)} products)")
            groups = self._group_by_similarity(brand, brand_products)
            all_groups.extend(groups)
        
        logger.info(f"\n✓ Grouping complete:")
        logger.info(f"  Total products (variants): {len(products)}")
        logger.info(f"  Product groups created:    {len(all_groups)}")
        logger.info(f"  Average variants per group: {len(products) / len(all_groups):.1f}")
        
        return all_groups
    
    def _group_by_brand(self, products: List[ProductData]) -> Dict[str, List[ProductData]]:
        """Group products by brand"""
        brand_groups = {}
        for product in products:
            brand = product.brand.strip()
            if brand not in brand_groups:
                brand_groups[brand] = []
            brand_groups[brand].append(product)
        
        return brand_groups
    
    def _group_by_similarity(
        self, 
        brand: str, 
        products: List[ProductData]
    ) -> List[ProductGroup]:
        """
        Group products with similar names together.
        
        Strategy:
        1. Extract base name (remove variant indicators)
        2. Calculate similarity between names
        3. Group similar products together
        """
        groups = []
        ungrouped = products.copy()
        
        while ungrouped:
            # Take first product as seed
            seed = ungrouped.pop(0)
            seed_base = self._extract_base_name(seed.name)
            
            # Create new group
            group = ProductGroup(
                base_name=seed_base,
                brand=brand
            )
            group.add_variant(seed)
            
            # Find similar products
            remaining = []
            for product in ungrouped:
                if self._is_similar(seed_base, product.name):
                    group.add_variant(product)
                    logger.debug(f"  Grouped: {product.name} → {seed_base}")
                else:
                    remaining.append(product)
            
            ungrouped = remaining
            groups.append(group)
            
            logger.debug(f"  Group '{seed_base}': {len(group)} variants")
        
        return groups
    
    def _extract_base_name(self, name: str) -> str:
        """
        Extract base product name by removing variant indicators.
        
        Examples:
        "Shampoo Black 50ml" → "Shampoo"
        "Lipstick Red #45" → "Lipstick"
        "Cream 100g Vanilla" → "Cream"
        """
        # Convert to lowercase for processing
        base = name.lower().strip()
        
        # Remove common variant patterns
        patterns = [
            r'\d+\s*(ml|g|oz|kg|l|mg|lb|fl\.oz)',  # Sizes: 50ml, 100g
            r'\d+\s*pack',  # Pack sizes
            r'#?\d+',  # Numbers and shade numbers
            r'\b(black|white|red|blue|green|yellow|pink|purple|brown|gray|grey|beige|nude|clear)\b',  # Colors
            r'\b(small|medium|large|xl|xxl|s|m|l)\b',  # Sizes
            r'\b(vanilla|chocolate|mint|rose|lavender|coconut|lemon|berry|fruit)\b',  # Flavors/scents
            r'\b(light|medium|dark|fair|deep)\b',  # Shades
            r'\b(matte|glossy|shimmer|metallic|satin)\b',  # Finishes
            r'\([^)]*\)',  # Remove parentheses content
            r'\-\s*\w+$',  # Remove trailing dash and word
        ]
        
        for pattern in patterns:
            base = re.sub(pattern, '', base, flags=re.IGNORECASE)
        
        # Clean up extra spaces and punctuation
        base = re.sub(r'\s+', ' ', base).strip()
        base = re.sub(r'[^\w\s]', '', base).strip()
        
        # If base is too short after cleaning, use first 2-3 words of original
        if len(base) < 3:
            words = name.split()[:3]
            base = ' '.join(words)
        
        return base.title()
    
    def _is_similar(self, base_name: str, product_name: str) -> bool:
        """
        Check if product name is similar to base name.
        
        Uses both fuzzy string matching and keyword matching.
        """
        # Extract base from product name
        product_base = self._extract_base_name(product_name)
        
        # Calculate similarity ratio
        ratio = SequenceMatcher(None, base_name.lower(), product_base.lower()).ratio()
        
        if ratio >= self.similarity_threshold:
            return True
        
        # Also check if base_name is a substring of product_name
        if base_name.lower() in product_name.lower():
            return True
        
        # Check if they share significant keywords (2+ words)
        base_words = set(base_name.lower().split())
        product_words = set(product_name.lower().split())
        
        # Remove common words
        common_words = {'the', 'a', 'an', 'of', 'for', 'with', 'and', 'or'}
        base_words -= common_words
        product_words -= common_words
        
        if len(base_words) >= 2 and base_words.issubset(product_words):
            return True
        
        return False
