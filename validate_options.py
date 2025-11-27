#!/usr/bin/env python3
"""
Validate that all variants of the same product have consistent Option Names.
This implements Rule 1 from fixer.instructions.md.
"""
import csv
import sys

def validate_option_consistency(csv_file):
    """Check that all variants of same product have identical Option Names"""
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    
    # Group by Handle
    handles = {}
    for row in rows:
        handle = row['Handle']
        if handle not in handles:
            handles[handle] = []
        handles[handle].append(row)
    
    print("=" * 80)
    print("SHOPIFY CSV VALIDATION - OPTION NAME CONSISTENCY (Rule 1)")
    print("=" * 80)
    
    errors = 0
    warnings = 0
    
    for handle, variants in handles.items():
        # Check Option Name consistency
        opt1_names = set(v['Option1 Name'] for v in variants)
        opt2_names = set(v['Option2 Name'] for v in variants)
        opt3_names = set(v['Option3 Name'] for v in variants)
        
        # Remove empty strings from sets for cleaner output
        opt1_names.discard('')
        opt2_names.discard('')
        opt3_names.discard('')
        
        # Check if consistent
        is_consistent = (
            len(opt1_names) <= 1 and 
            len(opt2_names) <= 1 and 
            len(opt3_names) <= 1
        )
        
        if not is_consistent:
            print(f"\n❌ ERROR - Product: {handle[:60]}")
            print(f"   Variants: {len(variants)} rows")
            if len(opt1_names) > 1:
                print(f"   Option1 Name INCONSISTENT: {opt1_names}")
            if len(opt2_names) > 1:
                print(f"   Option2 Name INCONSISTENT: {opt2_names}")
            if len(opt3_names) > 1:
                print(f"   Option3 Name INCONSISTENT: {opt3_names}")
            errors += 1
    
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Total products: {len(handles)}")
    print(f"Total rows: {len(rows)}")
    print(f"Errors found: {errors}")
    
    if errors == 0:
        print("\n✅ ALL PRODUCTS PASS - Ready for Shopify import!")
        return 0
    else:
        print(f"\n❌ {errors} PRODUCTS FAILED - Fix Option Name inconsistencies")
        return 1

if __name__ == '__main__':
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'data/output/shopify_products.csv'
    sys.exit(validate_option_consistency(csv_file))
