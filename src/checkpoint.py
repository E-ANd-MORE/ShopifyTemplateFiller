"""
Checkpoint Manager
Save and restore pipeline progress for recovery.
"""
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from src.models import ProductGroup, ProductData
from config import CACHE_DIR

logger = logging.getLogger(__name__)


class CheckpointManager:
    """
    Manage checkpoints for pipeline recovery.
    
    Saves progress after each batch to enable resuming if interrupted.
    """
    
    def __init__(self):
        self.checkpoint_dir = Path(CACHE_DIR)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    def save_checkpoint(
        self, 
        product_groups: List[ProductGroup], 
        batch_num: int,
        stats: dict = None
    ):
        """
        Save checkpoint after processing a batch.
        
        Args:
            product_groups: List of processed ProductGroup objects
            batch_num: Current batch number
            stats: Optional processing statistics
        """
        try:
            checkpoint_file = self.checkpoint_dir / f'checkpoint_batch_{batch_num}.json'
            
            # Convert product groups to serializable format
            data = {
                'batch_num': batch_num,
                'timestamp': datetime.now().isoformat(),
                'product_count': len(product_groups),
                'stats': stats or {},
                'product_groups': [self._serialize_group(g) for g in product_groups]
            }
            
            # Atomic write
            temp_file = checkpoint_file.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            temp_file.replace(checkpoint_file)
            
            logger.debug(f"Saved checkpoint for batch {batch_num} ({len(product_groups)} groups)")
            
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {str(e)}")
    
    def load_checkpoint(self, batch_num: int) -> Optional[List[ProductGroup]]:
        """
        Load checkpoint from previous run.
        
        Args:
            batch_num: Batch number to load
            
        Returns:
            List of ProductGroup objects, or None if not found
        """
        try:
            checkpoint_file = self.checkpoint_dir / f'checkpoint_batch_{batch_num}.json'
            
            if not checkpoint_file.exists():
                return None
            
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            product_groups = [
                self._deserialize_group(g) for g in data.get('product_groups', [])
            ]
            
            logger.info(f"Loaded checkpoint for batch {batch_num} ({len(product_groups)} groups)")
            return product_groups
            
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {str(e)}")
            return None
    
    def clear_checkpoints(self):
        """Clear all checkpoint files"""
        try:
            for checkpoint_file in self.checkpoint_dir.glob('checkpoint_*.json'):
                checkpoint_file.unlink()
            logger.info("Cleared all checkpoints")
        except Exception as e:
            logger.error(f"Failed to clear checkpoints: {str(e)}")
    
    def _serialize_group(self, group: ProductGroup) -> dict:
        """Convert ProductGroup to dict"""
        return {
            'base_name': group.base_name,
            'brand': group.brand,
            'url': group.url,
            'images': group.images,
            'description': group.description,
            'category': group.category,
            'tags': group.tags,
            'variants': [v.to_dict() for v in group.variants]
        }
    
    def _deserialize_group(self, data: dict) -> ProductGroup:
        """Convert dict to ProductGroup"""
        group = ProductGroup(
            base_name=data['base_name'],
            brand=data['brand']
        )
        group.url = data.get('url')
        group.images = data.get('images', [])
        group.description = data.get('description', '')
        group.category = data.get('category', 'Other')
        group.tags = data.get('tags', [])
        
        # Deserialize variants
        for variant_data in data.get('variants', []):
            variant = ProductData.from_dict(variant_data)
            group.add_variant(variant)
        
        return group
