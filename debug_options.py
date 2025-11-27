#!/usr/bin/env python3
"""Debug Option Names and Values in the CSV"""
import csv

with open('data/output/shopify_products.csv', 'r') as f:
    rows = list(csv.DictReader(f))

# Get first product's all rows
first_handle = rows[0]['Handle']
product_rows = [r for r in rows if r['Handle'] == first_handle]

print(f'Product: {first_handle}')
print(f'Total rows: {len(product_rows)}\n')

for i, row in enumerate(product_rows, 1):
    print(f'Row {i}:')
    title = row['Title'][:40] + '...' if row['Title'] else 'EMPTY'
    print(f"  Title: '{title}'")
    print(f"  Option1 Name: '{row['Option1 Name']}'  Value: '{row['Option1 Value']}'")
    print(f"  Option2 Name: '{row['Option2 Name']}'  Value: '{row['Option2 Value']}'")
    print(f"  Option3 Name: '{row['Option3 Name']}'  Value: '{row['Option3 Value']}'")
    img = row['Image Src'][:50] + '...' if row['Image Src'] else 'EMPTY'
    print(f"  Image: '{img}'")
    print()

# Check if there are any empty Option Names with non-empty Values
print("\n=== CHECKING FOR ISSUE: Empty Option Names with Values ===")
for i, row in enumerate(rows, 1):
    for opt_num in range(1, 4):
        name = row[f'Option{opt_num} Name']
        value = row[f'Option{opt_num} Value']
        
        # If Value exists but Name is empty, that's an error
        if value and not name:
            print(f"❌ Row {i} (Handle: {row['Handle'][:40]}...)")
            print(f"   Option{opt_num} Name is EMPTY but Value is '{value}'")
            print(f"   This causes: 'Option value provided for unknown options'")
