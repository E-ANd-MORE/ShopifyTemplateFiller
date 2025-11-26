"""
Pipeline Orchestrator
Main processing pipeline that coordinates all modules.
"""
import logging
import time
from datetime import datetime
from typing import List, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from src.models import ProductData, ProductGroup, ProcessingStats
from src.parser import ProductParser
from src.grouper import ProductGrouper
from src.tavily_searcher import TavilySearcher
from src.firecrawl_extractor import FirecrawlExtractor
from src.claude_enricher import ClaudeEnricher
from src.shopify_csv import ShopifyCSVGenerator
from src.checkpoint import CheckpointManager
from config import PROCESSING_CONFIG

logger = logging.getLogger(__name__)


class ProductEnrichmentPipeline:
    """
    Main pipeline orchestrator.
    
    Coordinates all modules to:
    1. Parse input CSV
    2. Group products by similarity
    3. Search for URLs (Tavily)
    4. Extract images (Firecrawl)
    5. Enrich with AI (Claude)
    6. Generate Shopify CSV
    
    Features:
    - Batch processing
    - Checkpoint/resume
    - Error recovery
    - Statistics tracking
    """
    
    def __init__(self):
        # Initialize all modules
        self.parser = ProductParser()
        self.grouper = ProductGrouper()
        self.searcher = TavilySearcher()
        self.extractor = FirecrawlExtractor()
        self.enricher = ClaudeEnricher()
        self.csv_gen = ShopifyCSVGenerator()
        self.checkpoint_mgr = CheckpointManager()
        
        # Configuration
        self.batch_size = PROCESSING_CONFIG['batch_size']
        self.max_workers = PROCESSING_CONFIG['max_workers']
        self.enable_checkpoints = PROCESSING_CONFIG['enable_checkpoints']
        
        logger.info("Pipeline initialized")
    
    def run(self, input_file: str, output_file: str) -> Tuple[bool, ProcessingStats]:
        """
        Run the complete pipeline.
        
        Args:
            input_file: Path to input CSV
            output_file: Path to output Shopify CSV
            
        Returns:
            Tuple of (success: bool, stats: ProcessingStats)
        """
        stats = ProcessingStats()
        stats.start_time = datetime.now().isoformat()
        start_time = time.time()
        
        try:
            logger.info("\n" + "=" * 80)
            logger.info("PRODUCT ENRICHMENT PIPELINE START")
            logger.info("=" * 80)
            logger.info(f"Input:  {input_file}")
            logger.info(f"Output: {output_file}")
            
            # Step 1: Parse input CSV
            logger.info("\n--- STEP 1: PARSING INPUT CSV ---")
            products, parse_stats = self.parser.parse_csv(input_file)
            
            # Update stats
            stats.total_rows_read = parse_stats['total_rows_read']
            stats.valid_products = parse_stats['valid_products']
            stats.skipped_duplicates = parse_stats['skipped_duplicates']
            stats.skipped_incomplete = parse_stats['skipped_incomplete']
            stats.parsing_errors = parse_stats['parsing_errors']
            
            if not products:
                logger.error("No products to process")
                return False, stats
            
            # Step 2: Group products by similarity
            logger.info("\n--- STEP 2: GROUPING PRODUCTS ---")
            product_groups = self.grouper.group_products(products)
            
            stats.total_product_groups = len(product_groups)
            stats.total_variants = len(products)
            
            if not product_groups:
                logger.error("No product groups created")
                return False, stats
            
            # Step 3: Process groups in batches
            logger.info(f"\n--- STEP 3: PROCESSING {len(product_groups)} PRODUCT GROUPS ---")
            logger.info(f"Batch size: {self.batch_size}")
            
            for batch_idx in range(0, len(product_groups), self.batch_size):
                batch = product_groups[batch_idx:batch_idx + self.batch_size]
                batch_num = (batch_idx // self.batch_size) + 1
                total_batches = (len(product_groups) + self.batch_size - 1) // self.batch_size
                
                logger.info(f"\n{'='*60}")
                logger.info(f"BATCH {batch_num}/{total_batches}")
                logger.info(f"Processing groups {batch_idx + 1} to {min(batch_idx + self.batch_size, len(product_groups))}")
                logger.info(f"{'='*60}")
                
                try:
                    self._process_batch(batch, stats)
                    
                    # Save checkpoint
                    if self.enable_checkpoints:
                        self.checkpoint_mgr.save_checkpoint(batch, batch_num, stats.to_dict())
                        
                except Exception as e:
                    logger.error(f"Batch {batch_num} failed: {str(e)}")
                    stats.add_error(f"Batch {batch_num}: {str(e)}")
                    continue
            
            # Step 4: Generate Shopify CSV
            logger.info("\n--- STEP 4: GENERATING SHOPIFY CSV ---")
            csv_content = self.csv_gen.generate_shopify_csv(product_groups)
            
            if not csv_content:
                logger.error("Failed to generate CSV")
                return False, stats
            
            csv_rows = len(csv_content.split('\n')) - 2
            stats.csv_rows_generated = csv_rows
            
            # Step 5: Validate output
            logger.info("\n--- STEP 5: VALIDATING OUTPUT ---")
            if not self._validate_output(csv_content):
                logger.error("Output validation failed")
                stats.add_error("Output validation failed")
                return False, stats
            
            # Step 6: Write to file
            logger.info("\n--- STEP 6: WRITING OUTPUT FILE ---")
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8', newline='\n') as f:
                f.write(csv_content)
            
            logger.info(f"✓ Saved to: {output_path}")
            
            # Final statistics
            stats.end_time = datetime.now().isoformat()
            stats.processing_time_sec = time.time() - start_time
            
            stats.print_report()
            
            return True, stats
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            stats.add_error(str(e))
            stats.end_time = datetime.now().isoformat()
            stats.processing_time_sec = time.time() - start_time
            return False, stats
    
    def _process_batch(self, batch: List[ProductGroup], stats: ProcessingStats):
        """
        Process a single batch through all enrichment steps.
        
        Args:
            batch: List of ProductGroup objects
            stats: Statistics object to update
        """
        # Phase 1: Search for URLs (sequential with rate limiting)
        logger.info("\n→ Phase 1: Searching for product URLs...")
        for group in batch:
            try:
                # Use first variant's info for search
                primary = group.get_primary_variant()
                if not primary:
                    continue
                
                url = self.searcher.search_url(group.brand, group.base_name)
                if url:
                    group.url = url
                    logger.debug(f"  ✓ {group.base_name}")
                else:
                    logger.debug(f"  ✗ {group.base_name} (URL not found)")
                    stats.failed_url_search += 1
                    
            except Exception as e:
                logger.error(f"  ✗ {group.base_name}: {str(e)}")
                stats.failed_url_search += 1
        
        # Phase 2: Extract images (parallel)
        logger.info("\n→ Phase 2: Extracting product images (parallel)...")
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self.extractor.extract_images,
                    group.url,
                    group.base_name
                ): group for group in batch if group.url
            }
            
            for future in as_completed(futures):
                group = futures[future]
                try:
                    images = future.result()
                    if images:
                        group.images = images
                        stats.total_images += len(images)
                        logger.debug(f"  ✓ {group.base_name} ({len(images)} images)")
                    else:
                        logger.debug(f"  ✗ {group.base_name} (no images)")
                        stats.failed_image_extraction += 1
                except Exception as e:
                    logger.error(f"  ✗ {group.base_name}: {str(e)}")
                    stats.failed_image_extraction += 1
        
        # Phase 3: Enrich with Claude (sequential)
        logger.info("\n→ Phase 3: Enriching with Claude AI...")
        for group in batch:
            try:
                primary = group.get_primary_variant()
                if not primary:
                    continue
                
                # Clean product name first
                cleaned_name = self.enricher.clean_product_name(
                    group.base_name,
                    group.brand
                )
                group.base_name = cleaned_name
                
                # Generate description
                group.description = self.enricher.generate_description(
                    group.brand,
                    cleaned_name,
                    primary.price
                )
                
                # Assign category
                group.category = self.enricher.assign_category(
                    group.brand,
                    cleaned_name
                )
                
                # Generate tags
                group.tags = self.enricher.generate_tags(
                    group.brand,
                    cleaned_name,
                    group.category
                )
                
                # Extract variants from each product in group
                for variant in group.variants:
                    variant.variants = self.enricher.extract_variants(variant.name)
                
                stats.successfully_processed += 1
                logger.debug(f"  ✓ {cleaned_name} (enriched)")
                
            except Exception as e:
                logger.error(f"  ✗ {group.base_name}: {str(e)}")
                stats.failed_enrichment += 1
    
    def _validate_output(self, csv_content: str) -> bool:
        """
        Validate final CSV before writing.
        
        Checks:
        - Valid CSV format
        - Has header and data rows
        - Required columns present
        - Sample row validation
        """
        try:
            lines = csv_content.split('\n')
            
            if len(lines) < 2:
                logger.error("CSV is empty")
                return False
            
            # Parse header
            import csv
            from io import StringIO
            reader = csv.DictReader(StringIO(csv_content))
            rows = list(reader)
            
            if not rows:
                logger.error("CSV has no data rows")
                return False
            
            # Check required columns
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
            
        except Exception as e:
            logger.error(f"CSV validation error: {str(e)}")
            return False
