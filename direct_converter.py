"""
Direct CSV Converter - No API calls needed
Converts input CSV with all data directly to Shopify format
"""
import pandas as pd
import re
import unicodedata
import logging
from pathlib import Path
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def sanitize_handle(text: str) -> str:
    """Convert text to Shopify-compliant handle"""
    text = text.lower()
    text = ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )
    text = re.sub(r'[^a-z0-9\s\-]', '', text)
    text = re.sub(r'\s+', '-', text)
    text = re.sub(r'-+', '-', text)
    text = text.strip('-')
    return text[:255] if text else 'product'


def parse_image_urls(image_str):
    """Parse image URLs from the CSV field (handles multi-line URLs)"""
    if pd.isna(image_str) or not image_str:
        return []
    
    # Split by newline or "- " prefix
    urls = []
    lines = str(image_str).split('\n')
    for line in lines:
        line = line.strip()
        if line.startswith('- '):
            line = line[2:].strip()
        if line and line.startswith('http'):
            urls.append(line)
    
    return urls if urls else ([image_str.strip()] if image_str.strip().startswith('http') else [])


def convert_to_shopify(input_file: str, output_file: str):
    """Convert input CSV to Shopify format"""
    
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("DIRECT CSV CONVERTER - NO API CALLS")
    logger.info("="*80)
    logger.info(f"Input:  {input_file}")
    logger.info(f"Output: {output_file}")
    
    # Read input CSV
    logger.info("\n📄 Reading input CSV...")
    df = pd.read_csv(input_file, encoding='utf-8-sig')
    logger.info(f"   Total products: {len(df)}")
    logger.info(f"   Unique brands: {df['PIM | Brand'].nunique()}")
    
    # Prepare Shopify rows
    shopify_rows = []
    seen_handles = set()
    
    logger.info("\n🔄 Converting to Shopify format...")
    
    for idx, row in df.iterrows():
        # Extract data
        brand = str(row['PIM | Brand']).strip()
        upc = str(row['UPC Code']).strip()
        english_desc = str(row['English Description']).strip()
        arabic_desc = str(row.get('Arabic Description', '')).strip()
        cost = float(row.get('COST', 0))
        tax = str(row.get('TAX  ', '')).strip()
        category = str(row.get('Category', 'Other')).strip()
        sub_category = str(row.get('Sub Category', '')).strip()
        
        # Parse images
        image1_urls = parse_image_urls(row.get('Image 1 URL'))
        image2_urls = parse_image_urls(row.get('Image 2 URL'))
        image3_urls = parse_image_urls(row.get('Image 3 URL'))
        all_images = image1_urls + image2_urls + image3_urls
        
        # Generate unique handle
        base_handle = sanitize_handle(f"{brand}-{english_desc}")
        handle = base_handle
        counter = 1
        while handle in seen_handles:
            handle = f"{base_handle}-{counter}"
            counter += 1
        seen_handles.add(handle)
        
        # Create description (English + Arabic)
        description = f"<p>{english_desc}</p>"
        if arabic_desc and arabic_desc != 'nan':
            description += f"<p dir='rtl'>{arabic_desc}</p>"
        
        # Determine if taxable
        taxable = 'TRUE' if '15%' in tax.upper() or 'TAX' in tax.upper() else 'FALSE'
        
        # First row with product info and first image
        first_row = {
            'Handle': handle,
            'Title': english_desc,
            'Body (HTML)': description,
            'Vendor': brand,
            'Product Category': category,
            'Type': sub_category if sub_category and sub_category != 'nan' else category,
            'Tags': f"{brand}, {category}, {sub_category}".replace(', nan', '').replace('nan, ', ''),
            'Published': 'TRUE',
            'Option1 Name': '',
            'Option1 Value': '',
            'Option2 Name': '',
            'Option2 Value': '',
            'Option3 Name': '',
            'Option3 Value': '',
            'Variant SKU': upc,
            'Variant Grams': '',
            'Variant Inventory Tracker': 'shopify',
            'Variant Inventory Qty': 1,
            'Variant Inventory Policy': 'deny',
            'Variant Fulfillment Service': 'manual',
            'Variant Price': cost,
            'Variant Compare At Price': '',
            'Variant Requires Shipping': 'TRUE',
            'Variant Taxable': taxable,
            'Variant Barcode': upc,
            'Image Src': all_images[0] if all_images else '',
            'Image Position': 1 if all_images else '',
            'Image Alt Text': english_desc,
            'Gift Card': 'FALSE',
            'SEO Title': english_desc[:70],
            'SEO Description': english_desc[:160],
            'Google Shopping / Google Product Category': category,
            'Google Shopping / Gender': '',
            'Google Shopping / Age Group': '',
            'Google Shopping / MPN': upc,
            'Google Shopping / Condition': 'new',
            'Google Shopping / Custom Product': 'FALSE',
            'Google Shopping / Custom Label 0': brand,
            'Google Shopping / Custom Label 1': category,
            'Google Shopping / Custom Label 2': '',
            'Google Shopping / Custom Label 3': '',
            'Google Shopping / Custom Label 4': '',
            'Variant Image': all_images[0] if all_images else '',
            'Variant Weight Unit': 'kg',
            'Variant Tax Code': '',
            'Cost per item': cost,
            'Included / International': 'International',
            'Price / International': cost,
            'Compare At Price / International': '',
            'Status': 'active'
        }
        shopify_rows.append(first_row)
        
        # Additional image rows (if more than 1 image)
        for img_idx in range(1, len(all_images)):
            image_row = {col: '' for col in first_row.keys()}
            image_row['Handle'] = handle
            image_row['Image Src'] = all_images[img_idx]
            image_row['Image Position'] = img_idx + 1
            image_row['Image Alt Text'] = english_desc
            shopify_rows.append(image_row)
        
        if (idx + 1) % 500 == 0:
            logger.info(f"   Processed {idx + 1}/{len(df)} products...")
    
    # Create DataFrame
    logger.info("\n📊 Creating Shopify CSV...")
    shopify_df = pd.DataFrame(shopify_rows)
    
    # Write to CSV
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    shopify_df.to_csv(output_path, index=False, encoding='utf-8', lineterminator='\n')
    
    # Stats
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("\n" + "="*80)
    logger.info("✅ CONVERSION COMPLETE")
    logger.info("="*80)
    logger.info(f"📦 Products processed: {len(df)}")
    logger.info(f"📝 Shopify CSV rows: {len(shopify_rows)}")
    logger.info(f"🔗 Unique handles: {len(seen_handles)}")
    logger.info(f"⏱️  Processing time: {elapsed:.1f}s")
    logger.info(f"📄 Output file: {output_path}")
    logger.info("\n🎉 Ready to import into Shopify!")
    logger.info("="*80)


if __name__ == '__main__':
    import sys
    
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'data/input/input.csv'
    output_file = 'data/output/shopify_products.csv'
    
    try:
        convert_to_shopify(input_file, output_file)
    except Exception as e:
        logger.error(f"❌ Conversion failed: {str(e)}", exc_info=True)
        sys.exit(1)
