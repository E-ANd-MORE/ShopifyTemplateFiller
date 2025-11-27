"""
Module 5: Shopify CSV Generator
Generate Shopify-compliant CSV from enriched product data.
"""
import logging
import csv
import re
import unicodedata
import pandas as pd
from typing import List, Dict, Set
from io import StringIO

from src.models import ProductGroup, ProductData

logger = logging.getLogger(__name__)


class ShopifyCSVGenerator:
    """
    Generate Shopify-compliant CSV from product groups.
    
    Key features:
    - Unique handle generation
    - Multiple variants per product
    - Multiple images per variant
    - UTF-8 encoding, Unix line endings
    - All required Shopify fields
    """
    
    # Shopify CSV column order (must match template exactly)
    SHOPIFY_COLUMNS = [
        'Handle', 'Title', 'Body (HTML)', 'Vendor', 'Product Category',
        'Type', 'Tags', 'Published', 
        'Option1 Name', 'Option1 Value', 'Option1 Linked To',
        'Option2 Name', 'Option2 Value', 'Option2 Linked To', 
        'Option3 Name', 'Option3 Value', 'Option3 Linked To',
        'Variant SKU', 'Variant Grams', 'Variant Inventory Tracker', 'Variant Inventory Policy',
        'Variant Fulfillment Service', 'Variant Price', 'Variant Compare At Price',
        'Variant Requires Shipping', 'Variant Taxable',
        'Unit Price Total Measure', 'Unit Price Total Measure Unit',
        'Unit Price Base Measure', 'Unit Price Base Measure Unit',
        'Variant Barcode', 'Image Src', 'Image Position', 'Image Alt Text',
        'Gift Card', 'SEO Title', 'SEO Description',
        'Google Shopping / Google Product Category', 'Google Shopping / Gender',
        'Google Shopping / Age Group', 'Google Shopping / MPN',
        'Google Shopping / Condition', 'Google Shopping / Custom Product',
        'Google Shopping / Custom Label 0', 'Google Shopping / Custom Label 1',
        'Google Shopping / Custom Label 2', 'Google Shopping / Custom Label 3',
        'Google Shopping / Custom Label 4',
        'Variant Image', 'Variant Weight Unit', 'Variant Tax Code',
        'Cost per item', 'Status'
    ]
    
    def __init__(self):
        self.seen_handles = set()
    
    def generate_shopify_csv(self, product_groups: List[ProductGroup]) -> str:
        """
        Generate Shopify CSV from product groups.
        
        Args:
            product_groups: List of ProductGroup objects
            
        Returns:
            CSV string ready to write to file
        """
        logger.info("\n" + "=" * 80)
        logger.info("GENERATING SHOPIFY CSV")
        logger.info("=" * 80)
        
        if not product_groups:
            logger.warning("No product groups to generate CSV")
            return ""
        
        rows = []
        self.seen_handles = set()
        
        for idx, group in enumerate(product_groups, 1):
            try:
                # Generate unique handle for this product group
                handle = self._generate_unique_handle(group)
                
                if not handle:
                    logger.warning(f"Failed to generate handle for: {group.base_name}")
                    continue
                
                # Generate rows for this product group
                product_rows = self._generate_product_rows(group, handle)
                rows.extend(product_rows)
                
                if idx % 50 == 0:
                    logger.debug(f"Generated CSV rows for {idx} products...")
                    
            except Exception as e:
                logger.error(f"Failed to generate rows for {group.base_name}: {str(e)}")
                continue
        
        if not rows:
            logger.error("No valid CSV rows generated")
            return ""
        
        # Create DataFrame
        df = pd.DataFrame(rows)
        
        # Ensure correct column order
        for col in self.SHOPIFY_COLUMNS:
            if col not in df.columns:
                df[col] = ''
        
        df = df[self.SHOPIFY_COLUMNS]
        
        # Convert to CSV string
        csv_string = df.to_csv(
            index=False,
            encoding='utf-8',
            lineterminator='\n',
            quoting=csv.QUOTE_MINIMAL
        )
        
        csv_rows = len(rows)
        logger.info(f"\n✓ CSV generation complete:")
        logger.info(f"  Product groups:  {len(product_groups)}")
        logger.info(f"  CSV rows:        {csv_rows}")
        logger.info(f"  Unique handles:  {len(self.seen_handles)}")
        
        return csv_string
    
    def _generate_unique_handle(self, group: ProductGroup) -> str:
        """
        Generate unique Shopify-compliant handle.
        
        Rules:
        - Max 255 characters
        - Lowercase letters, numbers, hyphens only
        - No leading/trailing hyphens
        - Must be unique
        """
        base_handle = self._sanitize_handle(f"{group.brand}-{group.base_name}")
        
        # If unique, return as-is
        if base_handle not in self.seen_handles:
            self.seen_handles.add(base_handle)
            return base_handle
        
        # Try with first variant's UPC
        if group.variants:
            first_upc = group.variants[0].upc_code
            handle_with_upc = f"{base_handle}-{first_upc[-4:]}"
            
            if handle_with_upc not in self.seen_handles:
                self.seen_handles.add(handle_with_upc)
                return handle_with_upc
            
            # Last resort: full UPC
            handle_final = f"{base_handle}-{first_upc}"
            if handle_final not in self.seen_handles:
                self.seen_handles.add(handle_final)
                return handle_final
        
        # Very last resort: add counter
        counter = 1
        while f"{base_handle}-{counter}" in self.seen_handles:
            counter += 1
        
        final_handle = f"{base_handle}-{counter}"
        self.seen_handles.add(final_handle)
        return final_handle
    
    def _sanitize_handle(self, text: str) -> str:
        """
        Convert text to Shopify-compliant handle.
        
        Examples:
        "Beauty™ System® MW-Capsules" → "beauty-system-mw-capsules"
        "Product (Old)" → "product-old"
        "Café Crème" → "cafe-creme"
        """
        # Convert to lowercase
        text = text.lower()
        
        # Remove accents
        text = ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != 'Mn'
        )
        
        # Remove non-alphanumeric except hyphens and spaces
        text = re.sub(r'[^a-z0-9\s\-]', '', text)
        
        # Replace spaces with hyphens
        text = re.sub(r'\s+', '-', text)
        
        # Collapse multiple hyphens
        text = re.sub(r'-+', '-', text)
        
        # Remove leading/trailing hyphens
        text = text.strip('-')
        
        # Ensure not too long
        text = text[:255]
        
        return text if text else 'product'
    
    def _generate_product_rows(self, group: ProductGroup, handle: str) -> List[Dict]:
        """
        Generate CSV rows for a product group.
        
        Strategy:
        - Each variant gets its own images from the input CSV
        - First variant gets full product info (title, description)
        - Subsequent variants only have handle + variant info
        - Each variant can have multiple image rows
        
        CRITICAL: All variants MUST have the same Option Names (Rule 1 from fixer.instructions.md)
        """
        rows = []
        
        if not group.variants:
            logger.warning(f"No variants for group: {group.base_name}")
            return rows
        
        # Shared data for ALL variants (same handle, no repeated title)
        shared_data = {
            'Handle': handle,
            'Vendor': group.brand,
            'Product Category': group.category or 'Other',
            'Type': group.category or '',
            'Tags': ','.join(group.tags) if group.tags else '',
            'Published': 'TRUE',
            'Status': 'active',
        }
        
        # CRITICAL FIX: Extract Option Names from variant with MOST options
        # All variants MUST use the same Option Names (Shopify Rule 1)
        # We need to find the variant with the most complete option set
        standard_option_names = self._extract_standard_option_names_from_all(group.variants)
        
        # Track image position across all variants
        image_position = 1
        
        # Process each variant
        for variant_idx, variant in enumerate(group.variants):
            # Extract variant VALUES using the STANDARD option names
            variant_options = self._extract_variant_options(variant, standard_option_names)
            
            # Get images for THIS specific variant (from input CSV)
            variant_images = variant.raw_images if hasattr(variant, 'raw_images') and variant.raw_images else []
            
            # First variant gets the product info
            is_first_variant = (variant_idx == 0)
            
            if is_first_variant:
                # First row includes product title and description
                row_data = shared_data.copy()
                row_data['Title'] = group.base_name
                row_data['Body (HTML)'] = group.description or ''
            else:
                # Subsequent rows: Keep Product Category consistent across all variants
                # Per Shopify Rule 2: All variants must have same Product Category
                row_data = {
                    'Handle': handle,
                    'Title': '',  # Empty for subsequent variants
                    'Body (HTML)': '',
                    'Vendor': group.brand,  # Keep vendor for all variants
                    'Product Category': group.category or 'Other',  # CRITICAL: Same category for all variants
                    'Type': group.category or '',  # Keep type consistent
                    'Tags': '',
                    'Published': '',
                    'Status': '',
                }
            
            # If this variant has images, create one row per image
            if variant_images:
                for img_idx, image_url in enumerate(variant_images):
                    row = self._create_variant_row(
                        row_data,
                        variant,
                        variant_options,
                        image_url,
                        image_position,
                        is_first=(img_idx == 0)  # Only first image row of this variant gets full data
                    )
                    rows.append(row)
                    image_position += 1
            else:
                # No images for this variant - create single row
                row = self._create_variant_row(
                    row_data,
                    variant,
                    variant_options,
                    None,
                    None,
                    is_first=True
                )
                rows.append(row)
        
        return rows
    
    def _extract_standard_option_names_from_all(self, variants: List[ProductData]) -> Dict[str, str]:
        """
        Extract and NORMALIZE Option Names from ALL variants.
        
        This ensures Rule 1 compliance: All variants MUST have identical Option Names.
        
        We collect all option names used across variants, normalize them (e.g., "Flavor/Scent" → "Flavor"),
        and only include options that appear in a meaningful number of variants.
        
        Args:
            variants: All variants in the product group
            
        Returns:
            Dict with standardized option names: {'Option1 Name': 'Color', 'Option2 Name': 'Size', ...}
        """
        # Count how many variants have each normalized option type
        option_counts = {}
        
        for variant in variants:
            seen_in_variant = set()  # Track which options this variant has
            if hasattr(variant, 'variants') and variant.variants:
                for var in variant.variants:
                    option_name = var.get('name', '').strip()
                    if option_name:
                        # Normalize the option name to a standard type
                        normalized = self._normalize_option_name(option_name)
                        seen_in_variant.add(normalized)
            
            # Count each unique option in this variant
            for opt in seen_in_variant:
                option_counts[opt] = option_counts.get(opt, 0) + 1
        
        # Only include options that appear in at least 30% of variants
        # OR if there are only 1-2 variants, include all options
        threshold = max(1, len(variants) * 0.3)
        
        standard_option_types = [
            opt for opt, count in sorted(option_counts.items())
            if count >= threshold
        ][:3]  # Max 3 options
        
        # Create standard names dict
        standard_names = {
            'Option1 Name': '',
            'Option2 Name': '',
            'Option3 Name': ''
        }
        
        for idx, option_type in enumerate(standard_option_types, 1):
            standard_names[f'Option{idx} Name'] = option_type
        
        return standard_names
    
    def _normalize_option_name(self, name: str) -> str:
        """
        Normalize option names to standard forms.
        
        Examples:
        - "Flavor/Scent" → "Flavor"
        - "Size/Volume" → "Size"
        - "Color/Shade" → "Color"
        """
        name_lower = name.lower()
        
        # Map variations to standard names
        if 'flavor' in name_lower or 'scent' in name_lower or 'fragrance' in name_lower:
            return 'Flavor'
        elif 'size' in name_lower or 'volume' in name_lower or 'weight' in name_lower:
            return 'Size'
        elif 'color' in name_lower or 'colour' in name_lower or 'shade' in name_lower:
            return 'Color'
        elif 'type' in name_lower or 'formula' in name_lower or 'finish' in name_lower:
            return 'Type'
        elif 'material' in name_lower or 'fabric' in name_lower:
            return 'Material'
        elif 'style' in name_lower or 'design' in name_lower:
            return 'Style'
        else:
            # Return the first word, capitalized
            first_word = name.split('/')[0].split()[0].strip()
            return first_word.capitalize()
    
    def _extract_variant_options(
        self, 
        variant: ProductData, 
        standard_option_names: Dict[str, str]
    ) -> Dict[str, str]:
        """
        Extract variant options using STANDARD option names.
        
        CRITICAL: This ensures all variants use the same Option Names (Shopify Rule 1).
        Only the Option VALUES differ between variants.
        
        We normalize the variant's option names and match them to standard names.
        IMPORTANT: If an Option Name is empty, the Value MUST also be empty.
        
        Args:
            variant: Current variant to extract values from
            standard_option_names: Standardized option names (normalized)
            
        Returns:
            Dict with option names and values matching standard names
        """
        options = standard_option_names.copy()  # Start with standard names
        
        # Initialize all values to empty
        options['Option1 Value'] = ''
        options['Option2 Value'] = ''
        options['Option3 Value'] = ''
        
        if hasattr(variant, 'variants') and variant.variants:
            # Build a map of normalized names to values from this variant
            variant_map = {}
            for var in variant.variants:
                raw_name = var.get('name', '').strip()
                value = var.get('value', '').strip()
                if raw_name and value:
                    normalized_name = self._normalize_option_name(raw_name)
                    variant_map[normalized_name] = value
            
            # Match each standard option to a value from variant
            for opt_num in range(1, 4):
                std_name = standard_option_names.get(f'Option{opt_num} Name', '').strip()
                if std_name and std_name in variant_map:
                    options[f'Option{opt_num} Value'] = variant_map[std_name]
                # If std_name not in variant_map, value stays empty
        
        return options
    
    def _create_variant_row(
        self,
        shared_data: Dict,
        variant: ProductData,
        variant_options: Dict,
        image_url: str = None,
        image_position: int = None,
        is_first: bool = True
    ) -> Dict:
        """Create a single CSV row for a variant"""
        row = shared_data.copy()
        
        # Variant options (from Claude extraction)
        for i in range(1, 4):
            row[f'Option{i} Name'] = variant_options.get(f'Option{i} Name', '')
            row[f'Option{i} Value'] = variant_options.get(f'Option{i} Value', '')
            row[f'Option{i} Linked To'] = ''  # Always empty for standard Shopify imports
        
        # SKU and inventory (only on first row per variant)
        if is_first:
            row['Variant SKU'] = variant.upc_code
            row['Variant Grams'] = ''
            row['Variant Inventory Tracker'] = 'shopify'
            row['Variant Inventory Policy'] = 'continue'
            row['Variant Fulfillment Service'] = 'manual'
            row['Variant Barcode'] = variant.upc_code
        else:
            row['Variant SKU'] = ''
            row['Variant Grams'] = ''
            row['Variant Inventory Tracker'] = ''
            row['Variant Inventory Policy'] = ''
            row['Variant Fulfillment Service'] = ''
            row['Variant Barcode'] = ''
        
        # Pricing
        row['Variant Price'] = float(variant.price)
        row['Variant Compare At Price'] = ''
        row['Variant Requires Shipping'] = 'TRUE'
        row['Variant Taxable'] = 'TRUE'
        
        # Unit pricing (empty for now)
        row['Unit Price Total Measure'] = ''
        row['Unit Price Total Measure Unit'] = ''
        row['Unit Price Base Measure'] = ''
        row['Unit Price Base Measure Unit'] = ''
        
        # Image
        row['Image Src'] = image_url if image_url else ''
        row['Image Position'] = image_position if image_position else ''
        row['Image Alt Text'] = f"{variant.brand} {variant.name}" if image_url else ''
        
        # Additional required fields
        row['Gift Card'] = 'FALSE'
        row['SEO Title'] = ''
        row['SEO Description'] = ''
        
        # Google Shopping fields (empty)
        row['Google Shopping / Google Product Category'] = ''
        row['Google Shopping / Gender'] = ''
        row['Google Shopping / Age Group'] = ''
        row['Google Shopping / MPN'] = ''
        row['Google Shopping / Condition'] = ''
        row['Google Shopping / Custom Product'] = ''
        row['Google Shopping / Custom Label 0'] = ''
        row['Google Shopping / Custom Label 1'] = ''
        row['Google Shopping / Custom Label 2'] = ''
        row['Google Shopping / Custom Label 3'] = ''
        row['Google Shopping / Custom Label 4'] = ''
        
        # Variant specific fields
        row['Variant Image'] = ''
        row['Variant Weight Unit'] = ''
        row['Variant Tax Code'] = ''
        row['Cost per item'] = ''
        
        return row
