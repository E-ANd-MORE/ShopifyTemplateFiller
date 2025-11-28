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
            
            all_output_files = []
            
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
                    
                    # Generate output CSV for this batch immediately
                    logger.info(f"\n→ Generating CSV for batch {batch_num}...")
                    batch_output_files = self._generate_batch_output(
                        batch, output_file, batch_num, total_batches, stats
                    )
                    
                    if batch_output_files:
                        all_output_files.extend(batch_output_files)
                        logger.info(f"✓ Generated {len(batch_output_files)} file(s) for batch {batch_num}")
                    else:
                        logger.warning(f"No output files generated for batch {batch_num}")
                        
                except Exception as e:
                    logger.error(f"Batch {batch_num} failed: {str(e)}")
                    stats.add_error(f"Batch {batch_num}: {str(e)}")
                    continue
            
            # Step 4: Summary
            logger.info("\n--- STEP 4: OUTPUT FILES SUMMARY ---")
            
            if not all_output_files:
                logger.error("No output files generated")
                return False, stats
            
            logger.info(f"\n✓ Generated {len(all_output_files)} total output file(s)")
            for i, file_path in enumerate(all_output_files, 1):
                logger.info(f"  {i}. {file_path}")
            
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
    
    def _enrich_single_group(self, group: ProductGroup) -> bool:
        """
        Enrich a single product group with Claude AI.
        
        Args:
            group: ProductGroup to enrich
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Small delay to prevent rate limiting (only 1-2 API calls now instead of 10!)
            time.sleep(0.3)
            
            primary = group.get_primary_variant()
            if not primary:
                return False
            
            # OPTIMIZED: Use batched enrichment (1 API call instead of 10)
            enriched = self.enricher.enrich_product_batch(
                group.brand,
                group.base_name,
                primary.price
            )
            
            # Apply enriched data to group
            group.base_name = enriched["cleaned_name"]
            group.description = enriched["description"]
            group.category = enriched["category"]
            group.tags = enriched["tags"]
            group.benefits = enriched["benefits"]
            group.ingredients = enriched["ingredients"]
            group.good_for = enriched["good_for"]
            group.suggested_usage = enriched["suggested_usage"]
            group.allergy_info = enriched["allergy_info"]
            
            # Extract variants from each product in group (separate call, needed per variant)
            for variant in group.variants:
                variant.variants = self.enricher.extract_variants(variant.name)
            
            logger.debug(f"  ✓ {group.base_name} (enriched with benefits)")
            return True
            
        except Exception as e:
            logger.error(f"  ✗ {group.base_name}: {str(e)}")
            return False
    
    def _process_batch(self, batch: List[ProductGroup], stats: ProcessingStats):
        """
        Process a single batch - SIMPLIFIED VERSION (NO API CALLS FOR URL/IMAGES).
        Images are already in the input CSV and mapped to variants.
        
        Args:
            batch: List of ProductGroup objects
            stats: Statistics object to update
        """
        # SKIP Phase 1 & 2: URLs and images already in input CSV
        logger.info("\n→ Skipping URL/Image fetching (using images from input CSV)")
        
        # Collect images from variants in each group
        for group in batch:
            all_images = []
            for variant in group.variants:
                if hasattr(variant, 'raw_images') and variant.raw_images:
                    all_images.extend(variant.raw_images)
            # Remove duplicates while preserving order
            seen = set()
            group.images = [img for img in all_images if img and img not in seen and not seen.add(img)]
            if group.images:
                stats.total_images += len(group.images)
        
        # Phase 3: Enrich with Claude (parallel for speed)
        logger.info("\n→ Enriching with Claude AI (parallel)...")
        
        if PROCESSING_CONFIG.get('parallel_enrichment', False):
            # Parallel enrichment using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(self._enrich_single_group, group): group for group in batch}
                
                for future in as_completed(futures):
                    group = futures[future]
                    try:
                        success = future.result()
                        if success:
                            stats.successfully_processed += 1
                        else:
                            stats.failed_enrichment += 1
                    except Exception as e:
                        logger.error(f"  ✗ {group.base_name}: {str(e)}")
                        stats.failed_enrichment += 1
        else:
            # Sequential enrichment (original behavior)
            for group in batch:
                try:
                    success = self._enrich_single_group(group)
                    if success:
                        stats.successfully_processed += 1
                    else:
                        stats.failed_enrichment += 1
                except Exception as e:
                    logger.error(f"  ✗ {group.base_name}: {str(e)}")
                    stats.failed_enrichment += 1
    
    def _generate_batch_output(
        self,
        batch: List[ProductGroup],
        base_output_file: str,
        batch_num: int,
        total_batches: int,
        stats: ProcessingStats
    ) -> List[str]:
        """
        Generate CSV output files for a single batch.
        
        Args:
            batch: Product groups in this batch
            base_output_file: Base output file path
            batch_num: Current batch number
            total_batches: Total number of batches
            stats: Statistics object
            
        Returns:
            List of generated file paths
        """
        try:
            output_path = Path(base_output_file)
            output_dir = output_path.parent
            output_base = output_path.stem
            output_ext = output_path.suffix
            
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate filename with batch number
            if total_batches == 1:
                batch_output_file = output_path
            else:
                batch_output_file = output_dir / f"{output_base}_batch{batch_num:03d}{output_ext}"
            
            # Generate CSV for this batch
            csv_content = self.csv_gen.generate_shopify_csv(batch)
            
            if not csv_content:
                logger.error(f"Failed to generate CSV for batch {batch_num}")
                return []
            
            # Parse and count rows
            import csv
            from io import StringIO
            reader = csv.DictReader(StringIO(csv_content))
            rows = list(reader)
            row_count = len(rows)
            
            # Now split this batch's output into files based on records_per_file
            from config import PROCESSING_CONFIG
            records_per_file = PROCESSING_CONFIG.get('records_per_file', 1000)
            
            # If batch rows fit in one file or no splitting needed
            if row_count <= records_per_file:
                # Write single file
                with open(batch_output_file, 'w', encoding='utf-8', newline='') as f:
                    f.write(csv_content)
                
                logger.debug(f"  Wrote {row_count} rows to {batch_output_file.name}")
                stats.csv_rows_generated += row_count
                stats.output_files_generated += 1
                
                return [str(batch_output_file)]
            
            else:
                # Split into multiple files
                output_files = []
                header = reader.fieldnames
                num_files = (row_count + records_per_file - 1) // records_per_file
                
                for file_idx in range(num_files):
                    start_idx = file_idx * records_per_file
                    end_idx = min(start_idx + records_per_file, row_count)
                    file_rows = rows[start_idx:end_idx]
                    
                    # Generate filename
                    if num_files == 1:
                        file_path = batch_output_file
                    else:
                        file_path = output_dir / f"{output_base}_batch{batch_num:03d}_part{file_idx + 1:03d}{output_ext}"
                    
                    # Write file
                    with open(file_path, 'w', encoding='utf-8', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=header)
                        writer.writeheader()
                        writer.writerows(file_rows)
                    
                    logger.debug(f"  Wrote {len(file_rows)} rows to {file_path.name}")
                    output_files.append(str(file_path))
                
                stats.csv_rows_generated += row_count
                stats.output_files_generated += len(output_files)
                
                return output_files
                
        except Exception as e:
            logger.error(f"Failed to generate output for batch {batch_num}: {str(e)}")
            return []
    
    def _generate_batched_csv_files(
        self, 
        product_groups: List[ProductGroup], 
        output_file: str,
        stats: ProcessingStats
    ) -> List[str]:
        """
        Generate multiple CSV files with configurable max records each.
        
        Args:
            product_groups: All product groups to export
            output_file: Base output file path
            stats: Statistics object to update
            
        Returns:
            List of generated file paths
        """
        from config import PROCESSING_CONFIG
        records_per_file = PROCESSING_CONFIG.get('records_per_file', 1000)
        output_path = Path(output_file)
        output_dir = output_path.parent
        output_base = output_path.stem
        output_ext = output_path.suffix
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate complete CSV content first
        logger.info("Generating complete CSV content...")
        csv_content = self.csv_gen.generate_shopify_csv(product_groups)
        
        if not csv_content:
            logger.error("Failed to generate CSV content")
            return []
        
        # Parse CSV content
        import csv
        from io import StringIO
        reader = csv.DictReader(StringIO(csv_content))
        header = reader.fieldnames
        all_rows = list(reader)
        
        total_rows = len(all_rows)
        logger.info(f"Total records: {total_rows}")
        
        if total_rows == 0:
            logger.error("No data rows generated")
            return []
        
        # Calculate number of files needed
        num_files = (total_rows + records_per_file - 1) // records_per_file
        logger.info(f"Splitting into {num_files} file(s) ({records_per_file} records each)")
        
        output_files = []
        
        # Split into batches and write files
        for file_idx in range(num_files):
            start_idx = file_idx * records_per_file
            end_idx = min(start_idx + records_per_file, total_rows)
            batch_rows = all_rows[start_idx:end_idx]
            
            # Generate filename
            if num_files == 1:
                file_path = output_path
            else:
                file_path = output_dir / f"{output_base}_part{file_idx + 1:03d}{output_ext}"
            
            # Write batch to file
            with open(file_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=header)
                writer.writeheader()
                writer.writerows(batch_rows)
            
            logger.info(f"  ✓ {file_path.name}: {len(batch_rows)} records (rows {start_idx + 1}-{end_idx})")
            output_files.append(str(file_path))
        
        # Update stats
        stats.csv_rows_generated = total_rows
        stats.output_files_generated = len(output_files)
        
        # Validate first file
        logger.info("\n--- VALIDATING OUTPUT ---")
        with open(output_files[0], 'r', encoding='utf-8') as f:
            first_file_content = f.read()
        
        if not self._validate_output(first_file_content):
            logger.error("Output validation failed")
            stats.add_error("Output validation failed")
            return []
        
        return output_files
    
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
