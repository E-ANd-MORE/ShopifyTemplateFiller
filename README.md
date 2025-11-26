# Product Enrichment Pipeline

Automated pipeline to enrich product data and generate Shopify-compliant CSV for import.

## Features

✅ **Parse CSV** - Handle multiple encodings, validate data  
✅ **Group Products** - Intelligently group variants together  
✅ **Search URLs** - Find product pages using Tavily API  
✅ **Extract Images** - Get product images via Firecrawl API  
✅ **AI Enrichment** - Generate descriptions, categories, tags with Claude  
✅ **Shopify CSV** - Generate 100% Shopify-compliant import file  
✅ **Error Recovery** - Checkpoints, retry logic, graceful degradation

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure API Keys

Create a `.env` file (copy from `.env.example`):

```bash
cp .env.example .env
```

Edit `.env` and add your API keys:

```env
TAVILY_API_KEY=your_tavily_api_key_here
FIRECRAWL_API_KEY=your_firecrawl_api_key_here
ANTHROPIC_API_KEY=your_anthropic_api_key_here
```

### 3. Prepare Input CSV

Place your input CSV in `data/input/`. Required columns:

- `PIM | Brand` - Product brand
- `UPC Code` - Unique product code
- `Name` - Product name (may contain variant info)
- `qty` - Quantity
- `PRICE` - Price

### 4. Run Pipeline

```bash
python main.py data/input/products.csv
```

Output will be generated at: `data/output/shopify_products.csv`

## Usage

### Basic Usage

```bash
python main.py <input_csv>
```

### Specify Output File

```bash
python main.py input.csv output.csv
```

### Advanced Options

```bash
python main.py input.csv --batch-size 20 --max-workers 10
```

Options:

- `--batch-size N` - Process N product groups per batch (default: 10)
- `--max-workers N` - Use N parallel workers for image extraction (default: 5)
- `--no-checkpoints` - Disable checkpoint saving

## How It Works

### Input CSV Format

Each row = one product variant:

```csv
PIM | Brand,UPC Code,Name,qty,PRICE
Brand X,001,Shampoo Black 50ml,10,15.99
Brand X,002,Shampoo Red 50ml,10,15.99
Brand X,003,Shampoo Black 100ml,8,25.99
```

### Pipeline Steps

1. **Parse CSV** - Read and validate input data
2. **Group Products** - Group similar products (e.g., "Shampoo" variants)
3. **Search URLs** - Find product pages via Tavily API
4. **Extract Images** - Get top 3 images per product via Firecrawl API
5. **Enrich Data** - Generate descriptions, categories, tags via Claude AI
6. **Generate CSV** - Create Shopify-compliant CSV with all variants

### Output Format

Shopify CSV with:

- One product per group
- Multiple variant rows per product
- All images included
- AI-generated descriptions and tags
- 100% Shopify import compatibility

## Project Structure

```
shopify/
├── config.py                   # Configuration
├── main.py                     # Entry point
├── requirements.txt            # Dependencies
├── .env                        # API keys (create this)
├── .env.example               # Template
├── src/
│   ├── models.py              # Data models
│   ├── parser.py              # CSV parsing
│   ├── grouper.py             # Product grouping
│   ├── tavily_searcher.py     # URL search
│   ├── firecrawl_extractor.py # Image extraction
│   ├── claude_enricher.py     # AI enrichment
│   ├── shopify_csv.py         # CSV generation
│   ├── checkpoint.py          # Recovery system
│   └── pipeline.py            # Orchestration
├── data/
│   ├── input/                 # Input CSV files
│   └── output/                # Generated Shopify CSVs
├── cache/                     # API response cache
└── logs/                      # Application logs
```

## API Keys

### Required APIs

1. **Tavily** - Product URL search

   - Sign up: https://tavily.com
   - Get API key from dashboard

2. **Firecrawl** - Image extraction

   - Sign up: https://firecrawl.dev
   - Get API key from dashboard

3. **Anthropic Claude** - AI enrichment
   - Sign up: https://console.anthropic.com
   - Get API key from account settings

## Configuration

Edit `config.py` to customize:

- Batch sizes
- API timeouts
- Cache directories
- Domain priorities for search
- Product categories

## Caching

The pipeline caches:

- Tavily URL searches → `cache/tavily_cache.json`
- Firecrawl image extractions → `cache/firecrawl_cache.json`
- Claude AI responses → `cache/claude_cache.json`

This reduces API costs on subsequent runs.

## Error Handling

The pipeline is designed to NEVER crash:

- ✅ Retry logic with exponential backoff
- ✅ Graceful fallbacks for missing data
- ✅ Checkpoint system for recovery
- ✅ Detailed logging
- ✅ Statistics tracking

## Import to Shopify

After generation:

1. Go to Shopify Admin → Products
2. Click "Import" button
3. Upload the generated CSV
4. Review and confirm import

## Troubleshooting

### "Missing API key" error

- Ensure `.env` file exists with all three API keys

### "Input file not found"

- Check file path is correct
- Place file in `data/input/` directory

### "No products to process"

- Verify CSV has required columns
- Check CSV encoding (should be UTF-8)

### Pipeline interrupted

- Run again - checkpoints allow resume from last batch

## License

MIT
