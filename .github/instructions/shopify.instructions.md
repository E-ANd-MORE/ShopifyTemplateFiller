---
applyTo: "**"
---

# 🤖 GitHub Copilot System Instructions for Product Enrichment Pipeline

## Implementation Guide for Bug-Free, Shopify-Compatible Code

---

## 📋 Overview

This document contains detailed instructions for GitHub Copilot to generate high-quality, production-ready code for the Product Enrichment Pipeline. The output MUST be Shopify-compatible and directly uploadable.

**Critical Success Criteria:**

- ✅ Zero crashes during processing
- ✅ 100% Shopify CSV format compliance
- ✅ All product rows have required fields
- ✅ Images are valid, public HTTPS URLs
- ✅ No duplicate handles
- ✅ Proper Unicode/encoding handling
- ✅ Graceful error recovery

---

## 🎯 Part 1: Code Quality Standards

### 1.1 Error Handling Pattern (MANDATORY)

**Use this pattern for ALL API calls:**

```python
def api_call_with_retry(self, endpoint: str, params: Dict) -> Optional[Dict]:
    """
    Template for all API calls. MUST include:
    1. Try-except-finally
    2. Exponential backoff on rate limits
    3. Timeout handling
    4. Detailed logging
    5. Return None on failure (never crash)
    """
    for attempt in range(1, self.max_retries + 1):
        try:
            response = requests.post(
                endpoint,
                json=params,
                timeout=self.timeout,
                headers=self.headers
            )

            # Status code validation
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Rate limit
                wait_time = 2 ** (attempt - 1)
                logger.warning(f"Rate limited. Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"API error {response.status_code}: {response.text}")
                return None

        except requests.Timeout:
            logger.error(f"Timeout on attempt {attempt}/{self.max_retries}")
            if attempt < self.max_retries:
                time.sleep(2 ** (attempt - 1))
            continue

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None

    logger.error(f"Failed after {self.max_retries} attempts")
    return None
```

### 1.2 Logging Pattern (MANDATORY)

```python
# ALWAYS use structured logging
import logging

logger = logging.getLogger(__name__)

# Pattern for all operations:
logger.info(f"STARTING: {operation_name} for product: {product_name}")
try:
    # operation code
    logger.debug(f"DETAIL: {specific_step}")
    logger.info(f"SUCCESS: {operation_name} completed in {elapsed}s")
except Exception as e:
    logger.error(f"FAILED: {operation_name} - {str(e)}")
    logger.debug(f"TRACEBACK: ", exc_info=True)
```

### 1.3 Data Validation Pattern (MANDATORY)

```python
def validate_and_sanitize(self, data: Any, field_type: str) -> Optional[Any]:
    """
    ALL user-facing data MUST be validated:
    1. Type checking
    2. Length limits
    3. Character validation
    4. URL validation for images
    5. Price validation for numbers
    """
    if field_type == "url":
        if not isinstance(data, str):
            return None
        if not data.startswith(('http://', 'https://')):
            return None
        try:
            urlparse(data)
            return data
        except:
            return None

    elif field_type == "price":
        try:
            price = float(data)
            if price < 0 or price > 999999:
                return None
            return price
        except (ValueError, TypeError):
            return None

    elif field_type == "handle":
        # Shopify handle: lowercase, max 255 chars, alphanumeric + hyphens
        if not isinstance(data, str):
            return None
        handle = data.lower().strip()
        handle = re.sub(r'[^a-z0-9\-]', '', handle)
        handle = re.sub(r'-+', '-', handle)
        handle = handle.strip('-')
        return handle[:255] if len(handle) > 0 else None

    else:
        return data
```

### 1.4 Caching Pattern (MANDATORY)

```python
def _get_or_create_cache(self, cache_type: str) -> Dict:
    """
    ALL caching MUST:
    1. Use JSON format
    2. Be human-readable (pretty print)
    3. Have timestamps
    4. Be thread-safe
    5. Auto-save after each write
    """
    cache_file = Path(self.cache_dir) / f"{cache_type}_cache.json"

    if cache_file.exists():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Cache load failed: {e}")
            return {}
    return {}

def _save_cache(self, cache_type: str, cache_data: Dict):
    """Auto-save cache after modifications"""
    try:
        cache_file = Path(self.cache_dir) / f"{cache_type}_cache.json"
        # Write to temp file first, then rename (atomic operation)
        temp_file = cache_file.with_suffix('.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
        temp_file.replace(cache_file)
    except Exception as e:
        logger.error(f"Cache save failed: {e}")
```

---

## 🏛️ Part 2: Module-Specific Instructions

### 2.1 Module 1: ProductParser - EXACT SPECIFICATIONS

**Input CSV Format (REQUIRED):**

```
Columns:
- "PIM | Brand" (required, string)
- "UPC Code" (required, unique string)
- "Name" (required, string)
- "qty" (required, integer)
- "PRICE" (required, float)
- "TAX" (optional, string)
- "VAT%" (optional, string)
- "Total with VAT" (optional, float)
```

**Implementation Requirements:**

```python
class ProductParser:
    """MUST handle:
    1. Encoding: UTF-8 with BOM
    2. Whitespace: Trim all fields
    3. Duplicates: Skip, log warning
    4. Missing data: Skip entire row
    5. Data types: Validate and convert
    """

    def parse_csv(self, filepath: str) -> Tuple[List[ProductData], Dict[str, int]]:
        """
        Returns:
        - List of valid ProductData objects
        - Statistics dict with counts

        Statistics MUST include:
        - total_rows_read
        - valid_products
        - skipped_duplicates
        - skipped_incomplete
        - parsing_errors
        """

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
            # Handle different encodings
            for encoding in ['utf-8-sig', 'utf-8', 'iso-8859-1', 'cp1252']:
                try:
                    df = pd.read_csv(filepath, encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError("Could not decode CSV with any encoding")

            logger.info(f"Parsed CSV with encoding: {encoding}")
            logger.info(f"Columns: {list(df.columns)}")

            for idx, row in df.iterrows():
                stats['total_rows_read'] += 1

                # Validation checks
                if pd.isna(row.get('UPC Code')) or pd.isna(row.get('Name')):
                    stats['skipped_incomplete'] += 1
                    logger.debug(f"Row {idx}: Missing required fields")
                    continue

                upc = str(row['UPC Code']).strip()

                if not upc or upc == 'nan':
                    stats['skipped_incomplete'] += 1
                    continue

                if upc in seen_upcs:
                    stats['skipped_duplicates'] += 1
                    logger.debug(f"Row {idx}: Duplicate UPC {upc}")
                    continue

                seen_upcs.add(upc)

                try:
                    product = ProductData(
                        brand=str(row.get('PIM | Brand', '')).strip() or 'Unknown',
                        upc_code=upc,
                        name=str(row.get('Name', '')).strip(),
                        quantity=int(float(row.get('qty', 1))),
                        price=float(row.get('PRICE', 0)),
                        tax=str(row.get('TAX  ', '')).strip() or 'No tax info',
                        vat_percentage=str(row.get('VAT%', '')).strip() or '0%',
                        total_with_vat=float(row.get('Total with VAT', 0))
                    )

                    # Validate product
                    if not self._validate_product(product):
                        stats['parsing_errors'] += 1
                        continue

                    products.append(product)
                    stats['valid_products'] += 1

                except Exception as e:
                    stats['parsing_errors'] += 1
                    logger.warning(f"Row {idx}: Parse error - {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"CSV parsing failed: {str(e)}")
            raise

        logger.info(f"Parsing complete: {stats}")
        return products, stats

    def _validate_product(self, product: ProductData) -> bool:
        """Validate individual product data"""
        # Name must not be empty
        if not product.name or len(product.name.strip()) == 0:
            return False

        # Brand should not be empty
        if not product.brand or product.brand == 'Unknown':
            return False

        # UPC must be unique and non-empty
        if not product.upc_code:
            return False

        # Price should be non-negative
        if product.price < 0:
            return False

        # Quantity should be positive
        if product.quantity <= 0:
            return False

        return True
```

### 2.2 Module 2: TavilySearcher - EXACT SPECIFICATIONS

**API Endpoint Requirements:**

```
POST https://api.tavily.com/search
Headers: Content-Type: application/json
Body: {
    "api_key": string,
    "query": string,
    "max_results": 5,
    "include_domains": [list of strings]
}
Response: {
    "results": [{
        "url": string,
        "title": string,
        "snippet": string
    }]
}
```

**Implementation Requirements:**

```python
class TavilySearcher:
    """MUST handle:
    1. Query construction: "{Brand} {ProductName} product"
    2. API rate limiting: 0.5s between requests
    3. Timeout: 30 seconds
    4. Cache: JSON format, auto-save
    5. Retry logic: 3 attempts with exponential backoff
    6. Domain prioritization
    """

    DOMAIN_PRIORITY = [
        # Try brand-specific domains first
        '{brand_domain}.com',
        '{brand_domain}.co',
        '{brand_domain}.jp',
        # Then major retailers
        'amazon.com',
        'sephora.com',
        'beautylish.com',
        'ulta.com',
    ]

    def search_url(self, brand: str, product_name: str) -> Optional[str]:
        """
        MUST return:
        - Valid HTTPS URL if found
        - None if not found (never crash)
        - Log all attempts
        """

        # Input validation
        if not brand or not product_name:
            logger.warning("Missing brand or product name")
            return None

        brand = brand.strip()
        product_name = product_name.strip()

        # Check cache first
        cache_key = self._generate_cache_key(brand, product_name)
        cached_url = self.cache.get(cache_key)

        if cached_url:
            logger.debug(f"Cache hit: {brand} - {product_name}")
            return cached_url

        # Construct query
        query = f"{brand} {product_name} product"

        logger.info(f"Searching: {query}")

        # Generate domain list
        brand_domain = brand.lower().replace(' ', '').replace('-', '')
        domains = [d.format(brand_domain=brand_domain) for d in self.DOMAIN_PRIORITY]

        # API call with retry
        for attempt in range(1, 4):
            try:
                payload = {
                    "api_key": self.api_key,
                    "query": query,
                    "max_results": 5,
                    "include_domains": domains
                }

                response = requests.post(
                    "https://api.tavily.com/search",
                    json=payload,
                    timeout=30
                )

                if response.status_code == 200:
                    data = response.json()
                    results = data.get('results', [])

                    if results:
                        url = results[0]['url']

                        # Validate URL
                        if self._validate_url(url):
                            self.cache[cache_key] = url
                            self._save_cache()
                            logger.info(f"Found: {url}")
                            return url

                    logger.debug(f"No results for: {query}")
                    return None

                elif response.status_code == 429:
                    wait_time = 2 ** attempt
                    logger.warning(f"Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue

                else:
                    logger.error(f"API error {response.status_code}")
                    return None

            except requests.Timeout:
                logger.warning(f"Timeout on attempt {attempt}")
                if attempt < 3:
                    time.sleep(2 ** attempt)
                continue

            except Exception as e:
                logger.error(f"Search failed: {str(e)}")
                return None

        logger.warning(f"Search failed after 3 attempts: {query}")
        return None

    def _validate_url(self, url: str) -> bool:
        """Validate URL format"""
        if not url.startswith(('http://', 'https://')):
            return False
        try:
            urlparse(url)
            return True
        except:
            return False

    def _generate_cache_key(self, brand: str, product: str) -> str:
        """Generate consistent cache key"""
        key = f"{brand.lower()}|{product.lower()}"
        return hashlib.md5(key.encode()).hexdigest()
```

### 2.3 Module 3: FirecrawlExtractor - EXACT SPECIFICATIONS

**API Endpoint Requirements:**

```
POST https://api.firecrawl.dev/v0/scrape
Headers:
  Authorization: Bearer {api_key}
  Content-Type: application/json
Body: {
    "url": string,
    "formats": ["html", "markdown"]
}
Response: {
    "success": boolean,
    "markdown": string,
    "html": string,
    "images": [{
        "src": string,
        "alt": string
    }]
}
```

**Image Filtering Algorithm (REQUIRED):**

```python
class FirecrawlExtractor:
    """MUST handle:
    1. URL validation: HTTPS only
    2. Image filtering: Relevance scoring
    3. Quality checks: Size, format, accessibility
    4. Timeout: 30 seconds
    5. Cache: URLs → image lists
    6. Rate limiting: 1 request per second
    """

    def extract_images(self, url: str, product_name: str) -> List[str]:
        """
        Returns:
        - List of valid, public HTTPS image URLs (max 3)
        - Empty list if extraction fails
        - Never returns invalid URLs
        """

        # Input validation
        if not url or not url.startswith(('http://', 'https://')):
            logger.warning(f"Invalid URL: {url}")
            return []

        # Check cache
        cached_images = self.cache.get(url)
        if cached_images is not None:
            logger.debug(f"Cache hit for images from: {url}")
            return cached_images

        logger.info(f"Extracting images from: {url}")

        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }

            payload = {
                "url": url,
                "formats": ["html", "markdown"]
            }

            response = requests.post(
                "https://api.firecrawl.dev/v0/scrape",
                json=payload,
                headers=headers,
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()

                if not data.get('success', False):
                    logger.warning(f"Firecrawl reported failure for: {url}")
                    return []

                # Extract images
                raw_images = data.get('images', [])

                if not raw_images:
                    logger.debug(f"No images found on: {url}")
                    return []

                # Filter and score images
                filtered_images = self._filter_images(raw_images, product_name)

                # Cache and return
                self.cache[url] = filtered_images
                self._save_cache()

                logger.info(f"Extracted {len(filtered_images)} images")
                return filtered_images

            else:
                logger.error(f"Firecrawl error {response.status_code}: {response.text}")
                return []

        except requests.Timeout:
            logger.error(f"Timeout extracting images from: {url}")
            return []

        except Exception as e:
            logger.error(f"Image extraction failed: {str(e)}")
            return []

    def _filter_images(self, images: List[Dict], product_name: str) -> List[str]:
        """
        Filtering logic MUST:
        1. Skip non-HTTPS URLs
        2. Skip logo/icon/badge images
        3. Score by relevance
        4. Return top 3
        5. All returned URLs MUST be valid, public HTTPS
        """

        scored_images = []
        product_keywords = product_name.lower().split()

        for img in images:
            src = img.get('src', '').strip()
            alt = img.get('alt', '').lower()

            # Must be HTTPS
            if not src.startswith('https://'):
                logger.debug(f"Skipping non-HTTPS image: {src}")
                continue

            # Skip common non-product images
            skip_patterns = [
                'logo', 'icon', 'badge', 'button',
                'arrow', 'star', 'rating', 'banner',
                'header', 'footer', 'nav', 'menu'
            ]

            if any(pattern in src.lower() for pattern in skip_patterns):
                logger.debug(f"Skipping non-product image: {src}")
                continue

            # Skip tracking pixels and tiny images
            if src.endswith(('gif', 'webp', '1x1', 'pixel')):
                continue

            # Score image
            score = 0

            # Alt text relevance (weight: 0.5)
            if alt:
                keyword_matches = sum(1 for kw in product_keywords if kw in alt)
                score += keyword_matches * 5

            # Image format preference (weight: 0.3)
            if src.lower().endswith('.jpg') or src.lower().endswith('.jpeg'):
                score += 3
            elif src.lower().endswith('.png'):
                score += 2

            # Common CDN indicators (weight: 0.2)
            if any(cdn in src.lower() for cdn in ['cdn', 'images', 'assets', 's3']):
                score += 1

            scored_images.append({
                'url': src,
                'score': score,
                'alt': alt
            })

        # Sort by score and return top 3
        scored_images.sort(key=lambda x: x['score'], reverse=True)
        result = [img['url'] for img in scored_images[:3]]

        logger.debug(f"Filtered {len(images)} images to {len(result)} valid images")
        return result
```

### 2.4 Module 4: ClaudeEnricher - EXACT SPECIFICATIONS

**Claude API Requirements:**

```
Model: claude-opus-4-1-20250805
Max Tokens: Varies by operation
Temperature: 0 (deterministic)
```

**Token Budgets (STRICT):**

```
- Variant extraction: 500 tokens
- Description: 300 tokens
- Category: 50 tokens
- Tags: 200 tokens
Total per product: 1,050 tokens
```

**Implementation Requirements:**

````python
class ClaudeEnricher:
    """MUST handle:
    1. Prompt safety: No injection attacks
    2. Token limits: Never exceed budget
    3. JSON parsing: Handle malformed responses
    4. Timeout: 30 seconds per call
    5. Cache: Same input = same output
    6. Fallback: Never crash, use defaults
    """

    # === OPERATION 1: VARIANT EXTRACTION ===
    def extract_variants(self, product: ProductData) -> List[Dict]:
        """
        MUST return:
        [{"name": str, "value": str}, ...]

        Examples:
        - {"name": "Color", "value": "Black"}
        - {"name": "Size", "value": "50ml"}
        - {"name": "Flavor", "value": "Mint"}

        On error: Return []
        """

        cache_key = f"variants|{product.name}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        prompt = f"""Extract all product variants from this name.

Product Name: {product.name}

Find any of these variant types:
- Color/Shade (e.g., Black, Blue, Red)
- Size/Volume (e.g., 50ml, 100g, L)
- Flavor/Scent (e.g., Mint, Rose, Vanilla)
- Type/Formula (e.g., Ammonia-Free, Organic)
- Strength/Level (e.g., Light, Medium, Heavy)
- Gender/Age (e.g., Men, Women, Unisex)

Return ONLY valid JSON array. Example:
[{{"name": "Color", "value": "Black"}}, {{"name": "Size", "value": "50ml"}}]

If no variants found, return: []"""

        try:
            message = self.client.messages.create(
                model="claude-opus-4-1-20250805",
                max_tokens=500,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )

            response_text = message.content[0].text.strip()

            # Parse JSON response
            variants = self._parse_json_response(response_text, default=[])

            # Validate variants
            variants = [
                v for v in variants
                if isinstance(v, dict) and 'name' in v and 'value' in v
            ]

            self.cache[cache_key] = variants
            self._save_cache()

            return variants

        except Exception as e:
            logger.error(f"Variant extraction failed: {str(e)}")
            return []

    # === OPERATION 2: DESCRIPTION GENERATION ===
    def generate_description(self, product: ProductData) -> str:
        """
        MUST return:
        - HTML-formatted string (2-3 sentences)
        - SEO-optimized
        - Professional tone

        On error: Return generic description
        """

        cache_key = f"description|{product.name}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        prompt = f"""Create a professional product description.

Brand: {product.brand}
Product: {product.name}
Price: ${product.price}

Requirements:
- 2-3 sentences only
- Highlight key benefits
- Professional e-commerce tone
- Include subtle keywords
- Use basic HTML if beneficial (<p>, <strong>)
- NO markdown, NO asterisks, NO backticks

Return ONLY the description text."""

        try:
            message = self.client.messages.create(
                model="claude-opus-4-1-20250805",
                max_tokens=300,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )

            description = message.content[0].text.strip()

            # Clean markdown artifacts if present
            description = description.replace('```', '')
            description = description.replace('**', '')
            description = description.replace('*', '')

            # Ensure max length
            if len(description) > 500:
                description = description[:497] + '...'

            self.cache[cache_key] = description
            self._save_cache()

            return description

        except Exception as e:
            logger.error(f"Description generation failed: {str(e)}")
            return f"Premium {product.brand} product. {product.name}."

    # === OPERATION 3: CATEGORY ASSIGNMENT ===
    def assign_category(self, product: ProductData) -> str:
        """
        MUST return one of:
        - "Hair Care"
        - "Skincare"
        - "Makeup"
        - "Bath & Body"
        - "Fragrance"
        - "Tools & Accessories"
        - "Other"
        """

        cache_key = f"category|{product.name}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        prompt = f"""Assign ONE product category.

Brand: {product.brand}
Product: {product.name}

Categories (MUST choose one):
1. Hair Care
2. Skincare
3. Makeup
4. Bath & Body
5. Fragrance
6. Tools & Accessories
7. Other

Return ONLY the category number (1-7) and name. Example: "1. Hair Care" """

        try:
            message = self.client.messages.create(
                model="claude-opus-4-1-20250805",
                max_tokens=50,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )

            response = message.content[0].text.strip()

            # Extract category name
            categories = [
                "Hair Care", "Skincare", "Makeup",
                "Bath & Body", "Fragrance", "Tools & Accessories", "Other"
            ]

            for cat in categories:
                if cat.lower() in response.lower():
                    self.cache[cache_key] = cat
                    self._save_cache()
                    return cat

            # Fallback
            return "Other"

        except Exception as e:
            logger.error(f"Category assignment failed: {str(e)}")
            return "Other"

    # === OPERATION 4: TAG GENERATION ===
    def generate_tags(self, product: ProductData) -> List[str]:
        """
        MUST return:
        - List of 6-10 tags
        - Lowercase
        - Hyphenated if multi-word
        - SEO-optimized

        On error: Return [brand, category]
        """

        cache_key = f"tags|{product.name}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        prompt = f"""Generate SEO tags for this product.

Brand: {product.brand}
Product: {product.name}

Requirements:
- 6-10 tags total
- One tag per line
- Lowercase
- Hyphenate multi-word tags
- Focus on searchability
- Include: brand, category, benefits, use-case

Format (exactly):
tag1
tag2
tag3
..."""

        try:
            message = self.client.messages.create(
                model="claude-opus-4-1-20250805",
                max_tokens=200,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )

            response = message.content[0].text.strip()

            # Parse tags
            tags = [
                tag.strip().lower()
                for tag in response.split('\n')
                if tag.strip() and not tag.startswith('#')
            ]

            # Validate tags
            tags = [
                tag for tag in tags
                if tag and 2 <= len(tag) <= 50
                and all(c in 'abcdefghijklmnopqrstuvwxyz0123456789- ' for c in tag)
            ]

            # Ensure 6-10 tags
            if len(tags) < 6:
                tags.extend([product.brand.lower(), product.category.lower()])
            tags = tags[:10]

            self.cache[cache_key] = tags
            self._save_cache()

            return tags

        except Exception as e:
            logger.error(f"Tag generation failed: {str(e)}")
            return [product.brand.lower(), "product", "beauty"]

    def _parse_json_response(self, response: str, default: Any = None) -> Any:
        """
        Safely parse JSON from Claude response.
        Handles cases where Claude wraps response in markdown code blocks.
        """
        try:
            # Try direct parse first
            return json.loads(response)
        except json.JSONDecodeError:
            # Try extracting JSON from markdown
            json_match = re.search(r'\[.*\]|\{.*\}', response, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group())
                except:
                    pass

            logger.debug(f"Failed to parse JSON: {response[:100]}")
            return default
````

### 2.5 Module 5: ShopifyCSVGenerator - EXACT SPECIFICATIONS

**Shopify CSV Format (MANDATORY FIELDS):**

```python
SHOPIFY_REQUIRED_FIELDS = {
    'Handle': str,                      # unique, lowercase, hyphenated
    'Title': str,                       # product name
    'Body (HTML)': str,                # html description
    'Vendor': str,                     # brand name
    'Product Category': str,           # e.g., "Hair Care"
    'Type': str,                       # product type
    'Tags': str,                       # comma-separated
    'Published': str,                  # "TRUE" or "FALSE"

    # Variant fields
    'Option1 Name': str,              # e.g., "Color"
    'Option1 Value': str,             # e.g., "Black"
    'Option1 Linked To': str,         # variant ID (usually empty)
    'Option2 Name': str,              # optional second option
    'Option2 Value': str,
    'Option3 Name': str,              # optional third option
    'Option3 Value': str,

    # Pricing
    'Variant Price': float,           # must be valid number
    'Variant Compare At Price': float, # original price (optional)
    'Variant Requires Shipping': str,  # "TRUE" or "FALSE"
    'Variant Taxable': str,           # "TRUE" or "FALSE"

    # Images
    'Image Src': str,                 # full HTTPS URL
    'Image Position': int,            # 1, 2, 3... per product
    'Image Alt Text': str,            # accessibility

    # SKU and inventory
    'SKU': str,                       # unique product code
    'Variant Barcode': str,           # barcode
    'Variant Fulfillment Service': str, # "manual"
    'Variant Inventory Tracker': str,  # "shopify"
    'Variant Inventory Qty': int,     # quantity
    'Variant Inventory Policy': str,  # "continue" or "deny"

    # Status
    'Status': str,                    # "active"
}
```

**Implementation Requirements:**

```python
class ShopifyCSVGenerator:
    """MUST handle:
    1. Handle uniqueness: NO duplicates
    2. Image URLs: MUST be valid HTTPS
    3. Price validation: Positive numbers
    4. Character encoding: UTF-8 with proper escaping
    5. Line endings: LF (Unix)
    6. BOM: No BOM
    7. Field ordering: Shopify standard
    """

    def generate_shopify_csv(self, products: List[ProductData]) -> str:
        """
        Returns:
        - CSV string ready to write to file
        - All rows valid for Shopify import
        - No duplicates

        Output format:
        - UTF-8 encoding
        - Unix line endings (\\n only, not \\r\\n)
        - Proper CSV quoting for special characters
        - One row per image OR one row per variant (whichever is more specific)
        """

        rows = []
        seen_handles = set()

        for product in products:
            # Generate unique handle
            handle = self._generate_unique_handle(product, seen_handles)

            if not handle:
                logger.warning(f"Failed to generate handle for: {product.name}")
                continue

            seen_handles.add(handle)

            # Generate product rows based on variants and images
            product_rows = self._generate_product_rows(product, handle)
            rows.extend(product_rows)

        # Create DataFrame with correct field order
        df = pd.DataFrame(rows)

        # Ensure correct column order (Shopify standard)
        column_order = [
            'Handle', 'Title', 'Body (HTML)', 'Vendor', 'Product Category',
            'Type', 'Tags', 'Published', 'Option1 Name', 'Option1 Value',
            'Option1 Linked To', 'Option2 Name', 'Option2 Value', 'Option3 Name',
            'Option3 Value', 'Variant Price', 'Variant Compare At Price',
            'Variant Requires Shipping', 'Variant Taxable', 'Image Src',
            'Image Position', 'Image Alt Text', 'SKU', 'Variant Barcode',
            'Variant Fulfillment Service', 'Variant Inventory Tracker',
            'Variant Inventory Qty', 'Variant Inventory Policy', 'Status'
        ]

        # Reorder columns, adding missing ones with empty values
        for col in column_order:
            if col not in df.columns:
                df[col] = ''

        df = df[column_order]

        # Convert to CSV string
        csv_string = df.to_csv(
            index=False,
            encoding='utf-8',
            line_terminator='\n',
            quoting=csv.QUOTE_MINIMAL
        )

        return csv_string

    def _generate_unique_handle(self, product: ProductData, seen_handles: set) -> str:
        """
        Generate unique, Shopify-compliant handle.

        Rules:
        - Max 255 characters
        - Lowercase letters, numbers, hyphens only
        - No leading/trailing hyphens
        - Must be unique across all products
        """

        base_handle = self._sanitize_handle(f"{product.brand}-{product.name}")

        # If unique, return as-is
        if base_handle not in seen_handles:
            return base_handle

        # If duplicate, append UPC code
        handle_with_upc = f"{base_handle}-{product.upc_code[-4:]}"

        if handle_with_upc not in seen_handles:
            return handle_with_upc

        # Last resort: append full UPC
        handle_final = f"{base_handle}-{product.upc_code}"
        if handle_final not in seen_handles:
            return handle_final

        logger.error(f"Could not generate unique handle for: {product.name}")
        return None

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

    def _generate_product_rows(self, product: ProductData, handle: str) -> List[Dict]:
        """
        Generate CSV rows for this product.

        Strategy:
        - If has variants: one row per variant
        - Add images to appropriate variant rows
        - If no variants: one row per image
        """

        rows = []

        if product.variants:
            # Case 1: Product has variants
            for variant in product.variants:
                # Create base row for this variant
                row = self._create_base_row(product, handle)

                # Add variant info
                row['Option1 Name'] = variant.get('name', '')
                row['Option1 Value'] = variant.get('value', '')

                # Add images to variant (one image per row)
                if product.images:
                    for image_idx, image_url in enumerate(product.images):
                        variant_row = row.copy()
                        variant_row['Image Src'] = image_url
                        variant_row['Image Position'] = image_idx + 1
                        rows.append(variant_row)
                else:
                    # Variant without images
                    rows.append(row)

        else:
            # Case 2: No variants, just images
            if product.images:
                for image_idx, image_url in enumerate(product.images):
                    row = self._create_base_row(product, handle)
                    row['Image Src'] = image_url
                    row['Image Position'] = image_idx + 1
                    rows.append(row)
            else:
                # No variants, no images
                row = self._create_base_row(product, handle)
                rows.append(row)

        return rows

    def _create_base_row(self, product: ProductData, handle: str) -> Dict:
        """
        Create base row with common product data.
        Subclasses can override to add more fields.
        """

        return {
            'Handle': handle,
            'Title': product.name,
            'Body (HTML)': product.description or '',
            'Vendor': product.brand,
            'Product Category': product.category or 'Other',
            'Type': product.category or '',
            'Tags': ','.join(product.tags) if product.tags else '',
            'Published': 'TRUE',

            'Option1 Name': '',
            'Option1 Value': '',
            'Option1 Linked To': '',
            'Option2 Name': '',
            'Option2 Value': '',
            'Option3 Name': '',
            'Option3 Value': '',

            'Variant Price': float(product.price),
            'Variant Compare At Price': '',
            'Variant Requires Shipping': 'TRUE',
            'Variant Taxable': 'TRUE',

            'Image Src': '',
            'Image Position': '',
            'Image Alt Text': f"{product.brand} {product.name}",

            'SKU': product.upc_code,
            'Variant Barcode': product.upc_code,
            'Variant Fulfillment Service': 'manual',
            'Variant Inventory Tracker': 'shopify',
            'Variant Inventory Qty': product.quantity,
            'Variant Inventory Policy': 'continue',
            'Status': 'active'
        }

    def _validate_row(self, row: Dict) -> bool:
        """
        Validate single row for Shopify compatibility.

        MUST check:
        1. Required fields present and non-empty
        2. Prices are valid numbers
        3. Image URLs are valid HTTPS
        4. Handles are unique
        5. No special characters that break CSV
        """

        # Required fields
        if not row.get('Handle'):
            logger.warning("Row missing Handle")
            return False

        if not row.get('Title'):
            logger.warning(f"Row {row['Handle']} missing Title")
            return False

        # Price validation
        try:
            price = float(row.get('Variant Price', 0))
            if price < 0 or price > 999999:
                return False
        except (ValueError, TypeError):
            logger.warning(f"Row {row['Handle']} has invalid price")
            return False

        # Image URL validation (if present)
        image_src = row.get('Image Src', '')
        if image_src:
            if not image_src.startswith('https://'):
                logger.warning(f"Row {row['Handle']} has non-HTTPS image")
                return False

        # Boolean fields
        boolean_fields = [
            'Variant Requires Shipping',
            'Variant Taxable',
            'Published'
        ]
        for field in boolean_fields:
            if row.get(field) not in ('TRUE', 'FALSE', ''):
                row[field] = 'TRUE'

        return True
```

---

## 🎯 Part 3: Main Orchestration Instructions

### 3.1 Pipeline Orchestrator

```python
class ProductEnrichmentPipeline:
    """Main orchestrator. MUST:
    1. Process batches sequentially
    2. Track statistics
    3. Handle partial failures
    4. Support resume on interruption
    5. Generate final report
    6. Validate final output
    """

    def run(self, input_file: str, output_file: str) -> Tuple[bool, Dict]:
        """
        Main entry point.

        Returns:
        - Success: bool (True if valid output generated)
        - Stats: dict with processing statistics
        """

        stats = {
            'start_time': datetime.now(),
            'total_products': 0,
            'successfully_processed': 0,
            'failed_url_search': 0,
            'failed_image_extraction': 0,
            'failed_enrichment': 0,
            'total_images': 0,
            'total_variants': 0,
            'csv_rows_generated': 0,
            'processing_time_sec': 0,
            'errors': []
        }

        try:
            # Step 1: Parse input
            logger.info("=" * 80)
            logger.info("PRODUCT ENRICHMENT PIPELINE START")
            logger.info("=" * 80)

            products, parse_stats = self.parser.parse_csv(input_file)
            stats.update(parse_stats)
            stats['total_products'] = len(products)

            if not products:
                logger.error("No products to process")
                return False, stats

            logger.info(f"Processing {len(products)} products in batches of {CONFIG['batch_size']}")

            # Step 2: Process in batches
            for batch_idx in range(0, len(products), CONFIG['batch_size']):
                batch = products[batch_idx:batch_idx + CONFIG['batch_size']]
                batch_num = (batch_idx // CONFIG['batch_size']) + 1

                logger.info(f"\n--- BATCH {batch_num} ---")
                logger.info(f"Processing products {batch_idx + 1} to {min(batch_idx + CONFIG['batch_size'], len(products))}")

                try:
                    self._process_batch(batch, stats)
                except Exception as e:
                    logger.error(f"Batch {batch_num} failed: {str(e)}")
                    stats['errors'].append(f"Batch {batch_num}: {str(e)}")
                    continue

            # Step 3: Generate final CSV
            logger.info("\n" + "=" * 80)
            logger.info("Generating Shopify CSV...")

            csv_content = self.csv_gen.generate_shopify_csv(products)
            csv_rows = len(csv_content.split('\n')) - 2  # Exclude header and empty line

            stats['csv_rows_generated'] = csv_rows

            # Step 4: Validate output
            if not self._validate_output(csv_content):
                logger.error("Output validation failed")
                return False, stats

            # Step 5: Write to file
            with open(output_file, 'w', encoding='utf-8', newline='\n') as f:
                f.write(csv_content)

            logger.info(f"Saved to: {output_file}")

            # Final report
            stats['processing_time_sec'] = (datetime.now() - stats['start_time']).total_seconds()
            self._print_final_report(stats)

            return True, stats

        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            stats['errors'].append(str(e))
            return False, stats

    def _process_batch(self, products: List[ProductData], stats: Dict):
        """Process single batch through all modules"""

        # Phase 1: Search for URLs
        logger.info("→ Searching for product URLs...")
        for product in products:
            try:
                product.url = self.searcher.search_url(product.brand, product.name)
                if product.url:
                    logger.debug(f"  ✓ {product.name}")
                else:
                    logger.debug(f"  ✗ {product.name} (URL not found)")
                    stats['failed_url_search'] += 1

                time.sleep(0.5)  # Rate limiting
            except Exception as e:
                logger.error(f"  ✗ {product.name}: {str(e)}")
                stats['failed_url_search'] += 1

        # Phase 2: Extract images (parallel)
        logger.info("→ Extracting product images (parallel)...")
        with ThreadPoolExecutor(max_workers=CONFIG['max_workers']) as executor:
            futures = {
                executor.submit(
                    self.extractor.extract_images,
                    product.url,
                    product.name
                ): product for product in products if product.url
            }

            for future in as_completed(futures):
                product = futures[future]
                try:
                    product.images = future.result()
                    if product.images:
                        stats['total_images'] += len(product.images)
                        logger.debug(f"  ✓ {product.name} ({len(product.images)} images)")
                    else:
                        logger.debug(f"  ✗ {product.name} (no images found)")
                        stats['failed_image_extraction'] += 1
                except Exception as e:
                    logger.error(f"  ✗ {product.name}: {str(e)}")
                    stats['failed_image_extraction'] += 1

        # Phase 3: Enrich with Claude
        logger.info("→ Enriching with Claude AI...")
        for product in products:
            try:
                product.description = self.enricher.generate_description(product)
                product.category = self.enricher.assign_category(product)
                product.tags = self.enricher.generate_tags(product)
                product.variants = self.enricher.extract_variants(product)

                if product.variants:
                    stats['total_variants'] += len(product.variants)

                stats['successfully_processed'] += 1
                logger.debug(f"  ✓ {product.name} ({len(product.variants or [])} variants)")

            except Exception as e:
                logger.error(f"  ✗ {product.name}: {str(e)}")
                stats['failed_enrichment'] += 1

    def _validate_output(self, csv_content: str) -> bool:
        """
        Validate final CSV before writing.

        Checks:
        1. Valid CSV format
        2. Has header row
        3. Has data rows
        4. All required columns present
        5. Sample row validation
        """

        lines = csv_content.split('\n')

        if len(lines) < 2:
            logger.error("CSV is empty")
            return False

        # Parse header
        try:
            reader = csv.DictReader(lines)
            rows = list(reader)
        except Exception as e:
            logger.error(f"CSV parsing failed: {str(e)}")
            return False

        if not rows:
            logger.error("CSV has no data rows")
            return False

        # Validate required columns
        required_cols = ['Handle', 'Title', 'Vendor', 'Variant Price']
        header = reader.fieldnames or []

        for col in required_cols:
            if col not in header:
                logger.error(f"Missing required column: {col}")
                return False

        # Spot check rows
        valid_rows = 0
        for row in rows[:min(10, len(rows))]:
            if row.get('Handle') and row.get('Title') and row.get('Vendor'):
                valid_rows += 1

        if valid_rows == 0:
            logger.error("No valid rows in sample")
            return False

        logger.info(f"✓ CSV validation passed ({len(rows)} rows)")
        return True

    def _print_final_report(self, stats: Dict):
        """Print final statistics"""

        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 80)
        logger.info(f"\n📊 STATISTICS:")
        logger.info(f"  Products processed:      {stats['successfully_processed']}/{stats['total_products']}")
        logger.info(f"  Failed URL searches:     {stats['failed_url_search']}")
        logger.info(f"  Failed image extraction: {stats['failed_image_extraction']}")
        logger.info(f"  Failed enrichment:       {stats['failed_enrichment']}")
        logger.info(f"  Total images extracted:  {stats['total_images']}")
        logger.info(f"  Total variants detected: {stats['total_variants']}")
        logger.info(f"  Shopify CSV rows:        {stats['csv_rows_generated']}")
        logger.info(f"  Processing time:         {stats['processing_time_sec']:.1f}s")

        if stats['errors']:
            logger.info(f"\n⚠️  ERRORS ({len(stats['errors'])}):")
            for error in stats['errors'][:10]:
                logger.info(f"  - {error}")

        logger.info("\n" + "=" * 80)
```

---

## ✅ Part 4: Output Validation Checklist

### 4.1 Before Writing CSV File

```python
VALIDATION_CHECKLIST = [
    # ✅ Format checks
    ("CSV format valid", lambda csv: len(csv.split('\n')) > 2),
    ("Has header row", lambda csv: 'Handle' in csv.split('\n')[0]),
    ("Has data rows", lambda csv: len(csv.split('\n')) > 10),

    # ✅ Content checks
    ("All handles unique", check_unique_handles),
    ("All handles non-empty", check_non_empty_handles),
    ("All prices are positive", check_positive_prices),
    ("All images are HTTPS", check_https_images),
    ("No duplicate rows", check_no_duplicates),

    # ✅ Shopify compatibility
    ("Required columns present", check_required_columns),
    ("Boolean fields correct", check_boolean_fields),
    ("Product titles present", check_product_titles),
]

def validate_csv_comprehensive(csv_content: str) -> Tuple[bool, List[str]]:
    """Run all validation checks"""
    errors = []

    for check_name, check_func in VALIDATION_CHECKLIST:
        try:
            if not check_func(csv_content):
                errors.append(f"FAILED: {check_name}")
        except Exception as e:
            errors.append(f"ERROR in {check_name}: {str(e)}")

    return len(errors) == 0, errors
```

### 4.2 Shopify Import Verification

```python
SHOPIFY_IMPORT_CHECKLIST = [
    "✅ CSV file has correct encoding (UTF-8)",
    "✅ No BOM (Byte Order Mark) in file",
    "✅ Line endings are LF (Unix), not CRLF (Windows)",
    "✅ All required columns present",
    "✅ All product handles are unique",
    "✅ All prices are valid positive numbers",
    "✅ All image URLs are public HTTPS",
    "✅ No special characters breaking CSV format",
    "✅ Proper HTML encoding in descriptions",
    "✅ Variants are properly structured",
    "✅ SKU/Barcode codes are unique per variant",
]
```

---

## 🚀 Part 5: Error Recovery & Robustness

### 5.1 Checkpoint & Resume System

```python
class CheckpointManager:
    """
    Save progress to resume on failure.
    MUST save after each batch.
    """

    def save_checkpoint(self, products: List[ProductData], batch_num: int):
        """Save processed products"""
        checkpoint_file = Path(CONFIG['cache_dir']) / f'checkpoint_batch_{batch_num}.json'

        data = {
            'batch_num': batch_num,
            'timestamp': datetime.now().isoformat(),
            'products': [p.to_dict() for p in products]
        }

        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def load_checkpoint(self, batch_num: int) -> Optional[List[ProductData]]:
        """Load previous batch"""
        checkpoint_file = Path(CONFIG['cache_dir']) / f'checkpoint_batch_{batch_num}.json'

        if checkpoint_file.exists():
            try:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return [ProductData(**p) for p in data['products']]
            except Exception as e:
                logger.error(f"Checkpoint load failed: {str(e)}")

        return None
```

### 5.2 Graceful Degradation

```python
# If API fails, use fallbacks
class FallbackStrategy:
    """
    When specific modules fail, use fallback data.
    NEVER crash the entire pipeline.
    """

    @staticmethod
    def fallback_description(brand: str, product_name: str) -> str:
        return f"Premium {brand} product. {product_name}."

    @staticmethod
    def fallback_category() -> str:
        return "Other"

    @staticmethod
    def fallback_tags(brand: str) -> List[str]:
        return [brand.lower(), "product", "beauty"]

    @staticmethod
    def fallback_variants() -> List[Dict]:
        return []
```

---

## 📝 Part 6: Documentation in Code

```python
# EVERY function and class MUST have docstrings following this format:
def important_function(param1: str, param2: int) -> str:
    """
    [One-line summary of what this does]

    [Detailed explanation if needed]

    Args:
        param1: Description, type, constraints
        param2: Description, type, constraints

    Returns:
        [Type]: [Description]
        None: [When None is returned and why]

    Raises:
        [ExceptionType]: [When raised]

    Examples:
        >>> result = important_function("test", 42)
        >>> assert result == "expected"

    Notes:
        - Important implementation detail
        - Performance consideration
        - Thread safety: [Thread-safe / Not thread-safe]
    """
```

---

## 🎯 Part 7: Final Checklist Before Deployment

```python
DEPLOYMENT_CHECKLIST = [
    # Code quality
    ☐ "All functions have docstrings",
    ☐ "All errors are caught and logged",
    ☐ "No print() calls (use logger instead)",
    ☐ "All imports are used",
    ☐ "No hardcoded values (use CONFIG)",

    # API integration
    ☐ "Tavily API calls retry on failure",
    ☐ "Firecrawl API calls retry on failure",
    ☐ "Claude API calls retry on failure",
    ☐ "Rate limiting implemented",
    ☐ "Timeout handling present",

    # Data validation
    ☐ "Input CSV is validated",
    ☐ "Product data is sanitized",
    ☐ "Image URLs are validated",
    ☐ "Prices are validated",
    ☐ "Handles are unique",

    # Shopify compatibility
    ☐ "CSV format matches Shopify spec",
    ☐ "All required columns present",
    ☐ "Proper UTF-8 encoding",
    ☐ "Unix line endings (LF)",
    ☐ "No BOM in output file",

    # Robustness
    ☐ "Caching implemented",
    ☐ "Error recovery works",
    ☐ "Progress tracking present",
    ☐ "Statistics are accurate",
    ☐ "Can resume from checkpoint",

    # Testing
    ☐ "Works with sample data",
    ☐ "Handles edge cases",
    ☐ "Validates output CSV",
    ☐ "Shopify can import output",
]
```

---

## 🎓 Implementation Summary for Copilot

**When Copilot generates code, it MUST:**

1. ✅ Follow all error handling patterns exactly as specified
2. ✅ Implement logging with structured format
3. ✅ Validate all inputs and outputs
4. ✅ Use the exact Shopify CSV schema provided
5. ✅ Cache all API responses
6. ✅ Handle rate limiting gracefully
7. ✅ Ensure output is directly importable to Shopify
8. ✅ Include comprehensive docstrings
9. ✅ Test with sample data before finalizing
10. ✅ Generate valid, uploadable Shopify CSV

**Key Philosophy:**
Never crash. Never lose data. Always provide fallback behavior. Log everything.

**Output Quality Guarantee:**
The generated CSV will be 100% Shopify-compatible and directly uploadable without any manual adjustments.

---

**End of System Instructions for GitHub Copilot**

Use these instructions when using GitHub Copilot to generate the implementation.
