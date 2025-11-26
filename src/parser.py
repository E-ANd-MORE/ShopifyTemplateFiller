"""
Module 1: Product Parser
Parses input CSV and creates ProductData objects for each row.
Each row represents ONE product variant.
"""
import logging
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
    - "Name" (required)
    - "qty" (required)
    - "PRICE" (required)
    - "TAX" (optional)
    - "VAT%" (optional)
    - "Total with VAT" (optional)
    
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
        """Validate that required columns exist"""
        required_columns = ['PIM | Brand', 'UPC Code', 'Name', 'qty', 'PRICE']
        missing = [col for col in required_columns if col not in df.columns]
        
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
    
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
        # Check for missing required fields
        if pd.isna(row.get('UPC Code')) or pd.isna(row.get('Name')):
            stats['skipped_incomplete'] += 1
            logger.debug(f"Row {idx + 2}: Missing UPC Code or Name")
            return None
        
        # Extract and clean UPC
        upc = str(row['UPC Code']).strip()
        if not upc or upc.lower() == 'nan':
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
        
        # Extract name
        name = str(row.get('Name', '')).strip()
        if not name or name.lower() == 'nan':
            stats['skipped_incomplete'] += 1
            logger.debug(f"Row {idx + 2}: Empty product name")
            return None
        
        # Extract quantity
        try:
            qty = int(float(row.get('qty', 1)))
            if qty <= 0:
                qty = 1
        except (ValueError, TypeError):
            qty = 1
            logger.debug(f"Row {idx + 2}: Invalid quantity, using 1")
        
        # Extract price
        try:
            price = float(row.get('PRICE', 0))
            if price < 0:
                logger.warning(f"Row {idx + 2}: Negative price {price}, using 0")
                price = 0
        except (ValueError, TypeError):
            logger.warning(f"Row {idx + 2}: Invalid price, using 0")
            price = 0
        
        # Extract optional fields
        tax = str(row.get('TAX  ', '')).strip()  # Note: double space in column name
        if tax.lower() == 'nan':
            tax = 'No tax info'
        
        vat = str(row.get('VAT%', '')).strip()
        if vat.lower() == 'nan':
            vat = '0%'
        
        try:
            total_vat = float(row.get('Total with VAT', 0))
        except (ValueError, TypeError):
            total_vat = 0.0
        
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
        
        # Validate product
        if not self._validate_product(product):
            stats['parsing_errors'] += 1
            logger.debug(f"Row {idx + 2}: Validation failed for {name}")
            return None
        
        logger.debug(f"Row {idx + 2}: ✓ Parsed {brand} - {name}")
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
