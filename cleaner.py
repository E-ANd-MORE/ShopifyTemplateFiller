import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time

# Load Excel file
file_path = "Pharma-products_cleaned.xlsx"
df = pd.read_excel(file_path)

# Drop 'Image' column if exists
if 'Image' in df.columns:
    df = df.drop(columns=['Image'])

# Drop rows where 'Name' column is empty
df = df[df['Name'].notna() & (df['Name'] != '')]

# Reset index
df = df.reset_index(drop=True)

# Replace empty strings with NaN
df = df.replace('', pd.NA)

# Select only the columns you listed
df = df[['Name', 'Code', 'Brand', 'Price', 'Details', 'Product Details', 'Quantity', 'URL', 'image URL']]

# Create new dataframe in the required format
df_new = pd.DataFrame({
    "PIM | Brand": df["Brand"],
    "UPC Code": df["Code"],
    "English Description": df["Details"],          # empty
    "COST": df["Price"],
  
})
print(len(df))
print(len(df_new))
 #PIM = empty
 # Brand = Brand
 # UPC Code = code 
 # Arabic Description = empty
 # English Description = empty
 # TAX = 15%
 # COST = price
 # GTIN (Internal ID) = empty
 # Category = empty
 # Sub Category = empty
 # Sub Sub Category = empty
 # Image 1 URL = image URL
 # Image 2 URL = empty
 # Image 3 URL = empty

# Limit to first 10 rows for testing

# Arabic Description //done, can be aggrated from english description if needed
# English Description //done, can be aggrated from arabic description if needed

# TAX //hard coded 15%?
# COST //is it the provided price? is it tax included or excluded?
# GTIN (Internal ID) //can be skipped 

# Category
# Sub Category
# Sub Sub Category

# Image 1 URL //done
# Image 2 URL //can be skipped 
# Image 3 URL //can be skipped 


#example row from input:
# Beauty System
#6955050960689
#,بيوتي سيستم غسول فم كبسولات بنكهه النعناع 12 قطعه × 13 م,
#Beauty System Mouth Wash Capsules With Mint Flavor 12 Pieces x 13 ml
# TAX 15%,
# 6.31
# ,,
# Oral Care
# Mouth Fresheneres
# Mouth Wash
# URL
# ,,
# ,,
# ,,


'''
Bigen
4549228123222
ين صبغه لحيه للرجال 80 جرام B 101 اسود طبيعي
,Bigen Beard Color 80g B 101 Natural Black
TAX 15%
35
,,
Personal Care
Men's Care
Beard Color
"https://cdn.files.smartsuite.com/AEHrSDUikTDqTbYuRTesYz/security=policy:eyJleHBpcnkiOjE3NTI5MjQ3NzE0MTcsImNhbGwiOlsicmVhZCIsImNvbnZlcnQiLCJwaWNrIiwic3RvcmUiXSwibWF4U2l6ZSI6NTAwMDAwMDAwMH0=
signature:ff21f1bbc61e95689ee074376b533de77d63e266af8e04eb4ebe121b35f6bafb/gGoekBiTRSGXGVxvRIAt"
,,
'''