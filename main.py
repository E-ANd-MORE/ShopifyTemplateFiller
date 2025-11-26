"""
Product Enrichment Pipeline - Main Entry Point

Usage:
    python main.py <input_csv> [output_csv]

Example:
    python main.py data/input/products.csv data/output/shopify_products.csv
"""
import sys
import argparse
import logging
import logging.config
from pathlib import Path

# Print immediately so user knows script started
print("=" * 80)
print("PRODUCT ENRICHMENT PIPELINE")
print("=" * 80)
print("Initializing...")

from config import LOGGING_CONFIG, INPUT_DIR, OUTPUT_DIR
from src.pipeline import ProductEnrichmentPipeline


def setup_logging():
    """Configure logging"""
    try:
        logging.config.dictConfig(LOGGING_CONFIG)
        print("✓ Logging configured")
    except Exception as e:
        print(f"⚠ Logging setup failed: {e}")
        # Setup basic logging as fallback
        logging.basicConfig(
            level=logging.INFO,
            format='%(levelname)s - %(message)s'
        )


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Product Enrichment Pipeline - Enrich products for Shopify import',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py input.csv
  python main.py input.csv output.csv
  python main.py data/input/products.csv data/output/shopify.csv

The pipeline will:
  1. Parse input CSV (products with variants)
  2. Group similar products together
  3. Search for product URLs (Tavily API)
  4. Extract product images (Firecrawl API)
  5. Enrich with AI (Claude API)
  6. Generate Shopify-compliant CSV

Required environment variables (in .env):
  - TAVILY_API_KEY
  - FIRECRAWL_API_KEY
  - ANTHROPIC_API_KEY
        """
    )
    
    parser.add_argument(
        'input_file',
        type=str,
        help='Path to input CSV file'
    )
    
    parser.add_argument(
        'output_file',
        type=str,
        nargs='?',
        default=None,
        help='Path to output Shopify CSV file (default: data/output/shopify_products.csv)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=None,
        help='Batch size for processing (default: from config)'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        default=None,
        help='Max parallel workers for image extraction (default: from config)'
    )
    
    parser.add_argument(
        '--no-checkpoints',
        action='store_true',
        help='Disable checkpoint saving'
    )
    
    return parser.parse_args()


def validate_environment():
    """Validate that required API keys are set"""
    print("\nValidating environment...")
    from config import TAVILY_API_KEY, FIRECRAWL_API_KEY, ANTHROPIC_API_KEY
    
    missing = []
    
    if not TAVILY_API_KEY:
        missing.append('TAVILY_API_KEY')
    
    if not FIRECRAWL_API_KEY:
        missing.append('FIRECRAWL_API_KEY')
    
    if not ANTHROPIC_API_KEY:
        missing.append('ANTHROPIC_API_KEY')
    
    if missing:
        print("\n❌ ERROR: Missing required API keys in .env file:")
        for key in missing:
            print(f"   - {key}")
        print("\nPlease create a .env file with your API keys.")
        print("See .env.example for template.")
        return False
    
    print("✓ All API keys found")
    return True


def main():
    """Main entry point"""
    print("\nStarting pipeline...")
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Parse arguments
    print("Parsing arguments...")
    args = parse_arguments()
    
    # Validate environment
    if not validate_environment():
        sys.exit(1)
    
    # Resolve input file path
    print(f"Resolving input file: {args.input_file}")
    input_file = Path(args.input_file)
    if not input_file.is_absolute():
        # Try relative to current directory first
        if input_file.exists():
            input_file = input_file.resolve()
        else:
            # Try relative to INPUT_DIR
            input_file = INPUT_DIR / input_file
    
    if not input_file.exists():
        print(f"❌ ERROR: Input file not found: {input_file}")
        logger.error(f"Input file not found: {input_file}")
        sys.exit(1)
    
    print(f"✓ Input file found: {input_file}")
    
    # Resolve output file path
    if args.output_file:
        output_file = Path(args.output_file)
        if not output_file.is_absolute():
            output_file = OUTPUT_DIR / output_file
    else:
        # Default output file
        output_file = OUTPUT_DIR / 'shopify_products.csv'
    
    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Override config if arguments provided
    if args.batch_size:
        from config import PROCESSING_CONFIG
        PROCESSING_CONFIG['batch_size'] = args.batch_size
    
    if args.max_workers:
        from config import PROCESSING_CONFIG
        PROCESSING_CONFIG['max_workers'] = args.max_workers
    
    if args.no_checkpoints:
        from config import PROCESSING_CONFIG
        PROCESSING_CONFIG['enable_checkpoints'] = False
    
    # Run pipeline
    try:
        print("\nInitializing pipeline modules...")
        pipeline = ProductEnrichmentPipeline()
        print("✓ Pipeline initialized\n")
        
        success, stats = pipeline.run(str(input_file), str(output_file))
        
        if success:
            logger.info("\n✅ Pipeline completed successfully!")
            logger.info(f"\n📄 Output file: {output_file}")
            logger.info("\nYou can now import this CSV into Shopify:")
            logger.info("  1. Go to Shopify Admin → Products")
            logger.info("  2. Click 'Import' button")
            logger.info(f"  3. Upload: {output_file.name}")
            sys.exit(0)
        else:
            logger.error("\n❌ Pipeline failed. Check logs for details.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("\n\n⚠️  Pipeline interrupted by user")
        sys.exit(130)
        
    except Exception as e:
        logger.error(f"\n❌ Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
