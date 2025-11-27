#!/usr/bin/env python3
"""
Complete Shopify CSV Validation
Checks all Shopify import requirements:
1. Product Categories (Standard Taxonomy)
2. Option Name Consistency
3. Option Value Validation
"""
import csv
import sys
from collections import defaultdict, Counter

def validate_complete(csv_file):
    """Complete validation for Shopify import readiness"""
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    
    print("=" * 80)
    print("SHOPIFY CSV COMPLETE VALIDATION")
    print("=" * 80)
    
    errors = []
    warnings = []
    
    # === VALIDATION 1: Product Categories ===
    print("\n📋 VALIDATION 1: Product Category Format")
    print("-" * 80)
    
    categories = set(r['Product Category'] for r in rows if r['Product Category'])
    
    for cat in categories:
        if '>' in cat:
            print(f"  ✓ {cat}")
        else:
            print(f"  ✗ {cat} - Missing '>' separator (not Shopify Standard Taxonomy)")
            errors.append(f"Invalid category format: {cat}")
    
    if not categories:
        warnings.append("No product categories assigned")
    
    # === VALIDATION 2: Option Name Consistency ===
    print("\n📋 VALIDATION 2: Option Name Consistency (Rule 1)")
    print("-" * 80)
    
    handles = defaultdict(list)
    for row in rows:
        handles[row['Handle']].append(row)
    
    option_errors = 0
    for handle, variants in handles.items():
        opt1_names = set(v['Option1 Name'] for v in variants)
        opt2_names = set(v['Option2 Name'] for v in variants)
        opt3_names = set(v['Option3 Name'] for v in variants)
        
        opt1_names.discard('')
        opt2_names.discard('')
        opt3_names.discard('')
        
        is_consistent = (
            len(opt1_names) <= 1 and 
            len(opt2_names) <= 1 and 
            len(opt3_names) <= 1
        )
        
        if not is_consistent:
            print(f"  ✗ {handle[:60]}...")
            if len(opt1_names) > 1:
                print(f"     Option1 Names: {opt1_names}")
            if len(opt2_names) > 1:
                print(f"     Option2 Names: {opt2_names}")
            if len(opt3_names) > 1:
                print(f"     Option3 Names: {opt3_names}")
            option_errors += 1
            errors.append(f"Inconsistent option names: {handle}")
    
    if option_errors == 0:
        print(f"  ✓ All {len(handles)} products have consistent option names")
    
    # === VALIDATION 3: Option Value Validation ===
    print("\n📋 VALIDATION 3: Option Name/Value Pairing")
    print("-" * 80)
    
    value_errors = 0
    for i, row in enumerate(rows, 1):
        for opt_num in range(1, 4):
            name = row[f'Option{opt_num} Name']
            value = row[f'Option{opt_num} Value']
            
            if not name and value:
                print(f"  ✗ Row {i}: Option{opt_num} has value '{value}' but no name")
                value_errors += 1
                errors.append(f"Row {i}: Empty Option{opt_num} Name with value")
    
    if value_errors == 0:
        print(f"  ✓ All {len(rows)} rows have valid option name/value pairs")
    
    # === VALIDATION 4: Required Fields ===
    print("\n📋 VALIDATION 4: Required Fields")
    print("-" * 80)
    
    required_fields = ['Handle', 'Title', 'Vendor', 'Variant Price']
    missing_count = 0
    
    for i, row in enumerate(rows, 1):
        missing = [f for f in required_fields if not row.get(f)]
        if missing:
            print(f"  ✗ Row {i}: Missing {', '.join(missing)}")
            missing_count += 1
            errors.append(f"Row {i}: Missing required fields")
    
    if missing_count == 0:
        print(f"  ✓ All rows have required fields")
    
    # === SUMMARY ===
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Total products: {len(handles)}")
    print(f"Total rows: {len(rows)}")
    print(f"Errors: {len(errors)}")
    print(f"Warnings: {len(warnings)}")
    
    if errors:
        print("\n❌ VALIDATION FAILED")
        print("\nErrors found:")
        for error in errors:
            print(f"  - {error}")
        return 1
    
    if warnings:
        print("\n⚠️  WARNINGS (CSV will import but review recommended):")
        for warning in warnings:
            print(f"  - {warning}")
    
    print("\n✅ CSV IS READY FOR SHOPIFY IMPORT")
    print("\nAll validations passed:")
    print("  ✓ Product categories use Shopify Standard Taxonomy")
    print("  ✓ Option names are consistent within each product")
    print("  ✓ No empty option names with values")
    print("  ✓ All required fields present")
    
    return 0

if __name__ == '__main__':
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'data/output/shopify_products.csv'
    sys.exit(validate_complete(csv_file))
