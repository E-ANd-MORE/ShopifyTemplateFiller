"""
Module 1: Product Parser
Parses input CSV and creates ProductData objects for each row.
Each row represents ONE product variant.
"""
import logging
import re
import pandas as pd
from typing import List, Tuple, Dict
from pathlib import Path

from src.models import ProductData, ProcessingStats

logger = logging.getLogger(__name__)


class ProductParser:
    """
    Parse input CSV file containing products and variants.
    
    Expected CSV columns:
    - "PIM | Brand" (required)
    - "UPC Code" (required, unique per variant)
    - "English Description" (required)
    - "COST" (required)
    - "TAX  " (optional)
    - "Category" (optional)
    - "Sub Category" (optional)
    - "Image 1 URL", "Image 2 URL", "Image 3 URL" (optional)
    
    Each row = one product variant with unique UPC.
    """
    
    def __init__(self):
        self.stats = ProcessingStats()
    
    def parse_csv(self, filepath: str) -> Tuple[List[ProductData], Dict[str, int]]:
        """
        Parse CSV file and return list of ProductData objects.
        
        Args:
            filepath: Path to input CSV file
            
        Returns:
            Tuple of (products list, statistics dict)
            
        Raises:
            ValueError: If CSV cannot be parsed
            FileNotFoundError: If file doesn't exist
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            raise FileNotFoundError(f"Input file not found: {filepath}")
        
        logger.info("=" * 80)
        logger.info(f"PARSING INPUT CSV: {filepath.name}")
        logger.info("=" * 80)
        
        stats = {
            'total_rows_read': 0,
            'valid_products': 0,
            'skipped_duplicates': 0,
            'skipped_incomplete': 0,
            'parsing_errors': 0
        }
        
        products = []
        seen_upcs = set()
        
        try:
            # Try different encodings
            df = None
            for encoding in ['utf-8-sig', 'utf-8', 'iso-8859-1', 'cp1252']:
                try:
                    df = pd.read_csv(filepath, encoding=encoding)
                    logger.info(f"✓ Parsed CSV with encoding: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                raise ValueError("Could not decode CSV with any supported encoding")
            
            logger.info(f"Columns found: {list(df.columns)}")
            logger.info(f"Total rows: {len(df)}")
            
            # Validate required columns
            self._validate_columns(df)
            
            # Process each row
            for idx, row in df.iterrows():
                stats['total_rows_read'] += 1
                
                try:
                    product = self._parse_row(row, idx, seen_upcs, stats)
                    if product:
                        products.append(product)
                        stats['valid_products'] += 1
                        
                        if stats['valid_products'] % 50 == 0:
                            logger.debug(f"Parsed {stats['valid_products']} products...")
                            
                except Exception as e:
                    stats['parsing_errors'] += 1
                    logger.warning(f"Row {idx + 2}: Parse error - {str(e)}")
                    continue
            
            logger.info(f"\n✓ Parsing complete:")
            logger.info(f"  Total rows read:      {stats['total_rows_read']}")
            logger.info(f"  Valid products:       {stats['valid_products']}")
            logger.info(f"  Skipped duplicates:   {stats['skipped_duplicates']}")
            logger.info(f"  Skipped incomplete:   {stats['skipped_incomplete']}")
            logger.info(f"  Parsing errors:       {stats['parsing_errors']}")
            
            return products, stats
            
        except Exception as e:
            logger.error(f"CSV parsing failed: {str(e)}")
            raise
    
    def _validate_columns(self, df: pd.DataFrame):
        """Validate that required columns exist - supports two formats"""
        # Format 1: Original format with Name, qty, PRICE
        format1 = ['PIM | Brand', 'UPC Code', 'Name', 'PRICE']
        # Format 2: New format with English Description, COST
        format2 = ['PIM | Brand', 'UPC Code', 'English Description', 'COST']
        
        missing1 = [col for col in format1 if col not in df.columns]
        missing2 = [col for col in format2 if col not in df.columns]
        
        if missing1 and missing2:
            raise ValueError(f"CSV must have either format 1 {format1} or format 2 {format2}")
    
    def _parse_row(
        self, 
        row: pd.Series, 
        idx: int, 
        seen_upcs: set, 
        stats: Dict
    ) -> ProductData:
        """
        Parse a single CSV row into ProductData object.
        
        Returns:
            ProductData object if valid, None if should be skipped
        """
        # Extract and clean UPC
        upc = str(row.get('UPC Code', '')).strip()
        if not upc or upc.lower() == 'nan' or pd.isna(row.get('UPC Code')):
            stats['skipped_incomplete'] += 1
            logger.debug(f"Row {idx + 2}: Empty UPC Code")
            return None
        
        # Check for duplicate UPC
        if upc in seen_upcs:
            stats['skipped_duplicates'] += 1
            logger.debug(f"Row {idx + 2}: Duplicate UPC {upc}")
            return None
        
        seen_upcs.add(upc)
        
        # Extract brand
        brand = str(row.get('PIM | Brand', '')).strip()
        if not brand or brand.lower() == 'nan':
            brand = 'Unknown'
            logger.debug(f"Row {idx + 2}: Missing brand, using 'Unknown'")
        
        # Extract name - try both column names (English Description or Name)
        name = str(row.get('English Description', row.get('Name', ''))).strip()
        if not name or name.lower() == 'nan':
            stats['skipped_incomplete'] += 1
            logger.debug(f"Row {idx + 2}: Empty product name")
            return None
        
        # Extract quantity - try qty column or default to 1
        try:
            qty = int(float(row.get('qty', 1)))
            if qty <= 0:
                qty = 1
        except (ValueError, TypeError):
            qty = 1
        
        # Extract cost
        try:
            cost = float(row.get('COST', row.get('PRICE', 0)))
            if cost < 0:
                logger.warning(f"Row {idx + 2}: Negative cost {cost}, using 0")
                cost = 0
        except (ValueError, TypeError):
            logger.warning(f"Row {idx + 2}: Invalid cost, using 0")
            cost = 0
        
        # Extract VAT percentage from TAX column
        tax = str(row.get('TAX  ', '')).strip()  # Note: double space in column name
        vat_percentage = 0.0
        
        if tax and tax.lower() != 'nan':
            # Parse VAT percentage from strings like "TAX 15%" or "15%"
            vat_match = re.search(r'(\d+(?:\.\d+)?)\s*%', tax)
            if vat_match:
                vat_percentage = float(vat_match.group(1))
        
        # Calculate final price: Cost + 65% markup + VAT
        # Formula: Price = Cost × (1 + 0.65) × (1 + VAT%)
        price_with_markup = cost * 1.65  # Add 65% markup
        price = price_with_markup * (1 + vat_percentage / 100)  # Add VAT
        
        # Store readable tax string
        if tax.lower() == 'nan' or not tax:
            tax = f'VAT {vat_percentage}%' if vat_percentage > 0 else 'No tax info'
        
        vat = f'{vat_percentage}%'
        total_vat = price  # Final price includes VAT
        
        # Extract image URLs from CSV
        images = []
        for img_col in ['Image 1 URL', 'Image 2 URL', 'Image 3 URL']:
            img_url = str(row.get(img_col, '')).strip()
            if img_url and img_url.lower() != 'nan' and img_url.startswith('http'):
                images.append(img_url)
        
        # Extract category info
        category = str(row.get('Category', '')).strip()
        if category.lower() == 'nan':
            category = ''
        
        sub_category = str(row.get('Sub Category', '')).strip()
        if sub_category.lower() == 'nan':
            sub_category = ''
        
        # Create ProductData object
        product = ProductData(
            brand=brand,
            upc_code=upc,
            name=name,
            quantity=qty,
            price=price,
            tax=tax,
            vat_percentage=vat,
            total_with_vat=total_vat
        )
        
        # Store images and category on the product object
        product.raw_images = images
        product.category = category if category else sub_category
        
        # Validate product
        if not self._validate_product(product):
            stats['parsing_errors'] += 1
            logger.debug(f"Row {idx + 2}: Validation failed for {name}")
            return None
        
        logger.debug(f"Row {idx + 2}: ✓ Parsed {brand} - {name} ({len(images)} images)")
        return product
    
    def _validate_product(self, product: ProductData) -> bool:
        """
        Validate individual product data.
        
        Returns:
            True if valid, False otherwise
        """
        # Name must not be empty
        if not product.name or len(product.name.strip()) == 0:
            return False
        
        # UPC must be non-empty
        if not product.upc_code:
            return False
        
        # Price should be non-negative
        if product.price < 0:
            return False
        
        # Quantity should be positive
        if product.quantity <= 0:
            return False
        
        return True
