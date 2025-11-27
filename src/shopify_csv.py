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
    
    # Shopify CSV column order (matching export template)
    SHOPIFY_COLUMNS = [
        'Handle', 'Title', 'Body (HTML)', 'Vendor', 'Product Category',
        'Type', 'Tags', 'Published', 'Option1 Name', 'Option1 Value',
        'Option2 Name', 'Option2 Value', 'Option3 Name', 'Option3 Value',
        'Variant Price', 'Variant Compare At Price', 'Variant Requires Shipping',
        'Variant Taxable', 'Image Src', 'Image Position', 'Image Alt Text',
        'SKU', 'Variant Barcode', 'Variant Fulfillment Service',
        'Variant Inventory Tracker', 'Variant Inventory Qty',
        'Variant Inventory Policy', 'Variant Image', 'Status'
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
        Generate CSV rows for a product group matching Shopify template.
        
        Strategy (matching actual Shopify export):
        - Row 1: Full product info + first variant + first image
        - Rows 2-N: Additional variants with images (continuing Image Position)
        - Rows N+1: Additional image-only rows (no variant data, just images)
        
        Each variant gets the product images, shown via Image Src and Variant Image columns.
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
        
        # Get all images
        images = group.images if group.images else []
        
        # Image position counter (increments across ALL rows)
        image_position = 1
        
        # Process each variant - each gets one row with one image
        for variant_idx, variant in enumerate(group.variants):
            variant_options = self._extract_variant_options(variant)
            is_first = (variant_idx == 0)
            
            # First variant gets full product info
            if is_first:
                row_data = shared_data.copy()
                row_data['Title'] = group.base_name
                row_data['Body (HTML)'] = group.description or ''
            else:
                # Subsequent variants: empty title/description
                row_data = {
                    'Handle': handle,
                    'Title': '',
                    'Body (HTML)': '',
                    'Vendor': '',
                    'Product Category': '',
                    'Type': '',
                    'Tags': '',
                    'Published': '',
                    'Status': '',
                }
            
            # Assign one image to this variant (cycle through images if more variants than images)
            if images:
                image_idx = variant_idx % len(images)
                image_url = images[image_idx]
                variant_image = images[0] if images else None  # Variant Image uses first image
            else:
                image_url = None
                variant_image = None
            
            row = self._create_variant_row(
                row_data,
                variant,
                variant_options,
                image_url,
                image_position if image_url else None,
                variant_image,
                is_first=is_first
            )
            rows.append(row)
            
            if image_url:
                image_position += 1
        
        # Add additional image-only rows (remaining images not assigned to variants)
        # This happens when we have more images than variants
        if len(images) > len(group.variants):
            for img_idx in range(len(group.variants), len(images)):
                # Image-only row: no variant data, just handle + image
                empty_row_data = {
                    'Handle': handle,
                    'Title': '',
                    'Body (HTML)': '',
                    'Vendor': '',
                    'Product Category': '',
                    'Type': '',
                    'Tags': '',
                    'Published': '',
                    'Status': '',
                }
                
                row = self._create_variant_row(
                    empty_row_data,
                    None,  # No variant
                    {},
                    images[img_idx],
                    image_position,
                    None,  # No variant image
                    is_first=False
                )
                rows.append(row)
                image_position += 1
        
        return rows
    
    def _extract_variant_options(self, variant: ProductData) -> Dict[str, str]:
        """
        Extract variant options from product's variant data.
        
        Returns dict with option names and values.
        """
        options = {}
        
        if variant.variants:
            # Use Claude-extracted variants (up to 3 options)
            for idx, var in enumerate(variant.variants[:3], 1):
                options[f'Option{idx} Name'] = var.get('name', '')
                options[f'Option{idx} Value'] = var.get('value', '')
        
        return options
    
    def _create_variant_row(
        self,
        shared_data: Dict,
        variant: ProductData,
        variant_options: Dict,
        image_url: str = None,
        image_position: int = None,
        variant_image: str = None,
        is_first: bool = True
    ) -> Dict:
        """Create a single CSV row for a variant"""
        row = shared_data.copy()
        
        # Handle case when variant is None (image-only rows)
        if variant is None:
            # Image-only row
            for i in range(1, 4):
                row[f'Option{i} Name'] = ''
                row[f'Option{i} Value'] = ''
            row['Variant Price'] = ''
            row['Variant Compare At Price'] = ''
            row['Variant Requires Shipping'] = ''
            row['Variant Taxable'] = ''
            row['SKU'] = ''
            row['Variant Barcode'] = ''
            row['Variant Fulfillment Service'] = ''
            row['Variant Inventory Tracker'] = ''
            row['Variant Inventory Qty'] = ''
            row['Variant Inventory Policy'] = ''
            row['Image Src'] = image_url if image_url else ''
            row['Image Position'] = image_position if image_position else ''
            row['Image Alt Text'] = ''
            row['Variant Image'] = ''
            return row
        
        # Variant options (from Claude extraction)
        for i in range(1, 4):
            row[f'Option{i} Name'] = variant_options.get(f'Option{i} Name', '')
            row[f'Option{i} Value'] = variant_options.get(f'Option{i} Value', '')
        
        # Pricing
        row['Variant Price'] = float(variant.price)
        row['Variant Compare At Price'] = ''
        row['Variant Requires Shipping'] = 'TRUE'
        row['Variant Taxable'] = 'TRUE'
        
        # Image
        row['Image Src'] = image_url if image_url else ''
        row['Image Position'] = image_position if image_position else ''
        row['Image Alt Text'] = f"{variant.brand} {variant.name}" if image_url else ''
        
        # Variant Image (specific image for this variant)
        row['Variant Image'] = variant_image if variant_image else ''
        
        # SKU and inventory (always present for variant rows)
        row['SKU'] = variant.upc_code if is_first else ''
        row['Variant Barcode'] = variant.upc_code if is_first else ''
        row['Variant Fulfillment Service'] = 'manual' if is_first else ''
        row['Variant Inventory Tracker'] = 'shopify' if is_first else ''
        row['Variant Inventory Qty'] = variant.quantity if is_first else ''
        row['Variant Inventory Policy'] = 'continue' if is_first else ''
        
        return row
