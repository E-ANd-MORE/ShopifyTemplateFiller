"""
Test script to verify batching logic splits files correctly
"""
import csv
from pathlib import Path

def simulate_batching(total_rows, records_per_file=1000):
    """Simulate the batching logic"""
    num_files = (total_rows + records_per_file - 1) // records_per_file
    
    print(f"Total rows: {total_rows}")
    print(f"Records per file: {records_per_file}")
    print(f"Number of files needed: {num_files}")
    print()
    
    for file_idx in range(num_files):
        start_idx = file_idx * records_per_file
        end_idx = min(start_idx + records_per_file, total_rows)
        num_records = end_idx - start_idx
        
        filename = f"shopify_products_part{file_idx + 1:03d}.csv"
        print(f"File {file_idx + 1}: {filename}")
        print(f"  Rows {start_idx + 1}-{end_idx} ({num_records} records)")
        print()

# Test scenarios
print("=" * 80)
print("BATCHING LOGIC TEST")
print("=" * 80)
print()

print("Scenario 1: Less than 1000 records (should create 1 file)")
print("-" * 80)
simulate_batching(9)

print("Scenario 2: Exactly 1000 records (should create 1 file)")
print("-" * 80)
simulate_batching(1000)

print("Scenario 3: 1001 records (should create 2 files)")
print("-" * 80)
simulate_batching(1001)

print("Scenario 4: 2500 records (should create 3 files)")
print("-" * 80)
simulate_batching(2500)

print("Scenario 5: 7823 records - actual input.csv size (should create 8 files)")
print("-" * 80)
simulate_batching(7823)
