"""
Data models for Product Enrichment Pipeline
"""
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any


@dataclass
class ProductData:
    """
    Represents a single product variant from input CSV.
    
    Each row in the input CSV becomes one ProductData object.
    Multiple ProductData objects with similar names will be grouped
    into a single Shopify product with multiple variants.
    """
    # Original CSV fields (required)
    brand: str
    upc_code: str
    name: str
    quantity: int
    price: float
    
    # Optional CSV fields
    tax: str = ""
    vat_percentage: str = ""
    total_with_vat: float = 0.0
    
    # Enriched fields (populated during processing)
    url: Optional[str] = None
    images: List[str] = field(default_factory=list)
    description: str = ""
    category: str = "Other"
    tags: List[str] = field(default_factory=list)
    variants: List[Dict[str, str]] = field(default_factory=list)
    
    # Grouping metadata
    product_group_id: Optional[str] = None  # For grouping similar products
    is_variant: bool = True  # Each row is a variant
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProductData':
        """Create from dictionary"""
        return cls(**data)
    
    def __str__(self) -> str:
        return f"ProductData(brand={self.brand}, name={self.name}, upc={self.upc_code})"
    
    def __repr__(self) -> str:
        return self.__str__()


@dataclass
class ProductGroup:
    """
    Represents a grouped product with multiple variants.
    
    This is used to combine multiple ProductData objects (CSV rows)
    that represent the same base product with different variants.
    """
    base_name: str
    brand: str
    variants: List[ProductData] = field(default_factory=list)
    
    # Shared enrichment data (same for all variants)
    url: Optional[str] = None
    images: List[str] = field(default_factory=list)
    description: str = ""
    category: str = "Other"
    tags: List[str] = field(default_factory=list)
    
    def add_variant(self, product: ProductData):
        """Add a variant to this product group"""
        self.variants.append(product)
        product.product_group_id = self.get_group_id()
    
    def get_group_id(self) -> str:
        """Generate unique group identifier"""
        return f"{self.brand}_{self.base_name}".lower().replace(" ", "_")
    
    def get_primary_variant(self) -> Optional[ProductData]:
        """Get the first variant as primary"""
        return self.variants[0] if self.variants else None
    
    def __len__(self) -> int:
        return len(self.variants)
    
    def __str__(self) -> str:
        return f"ProductGroup(brand={self.brand}, name={self.base_name}, variants={len(self.variants)})"


@dataclass
class ProcessingStats:
    """Statistics for pipeline processing"""
    start_time: str = ""
    end_time: str = ""
    processing_time_sec: float = 0.0
    
    # Parsing stats
    total_rows_read: int = 0
    valid_products: int = 0
    skipped_duplicates: int = 0
    skipped_incomplete: int = 0
    parsing_errors: int = 0
    
    # Grouping stats
    total_product_groups: int = 0
    total_variants: int = 0
    
    # Enrichment stats
    successfully_processed: int = 0
    failed_url_search: int = 0
    failed_image_extraction: int = 0
    failed_enrichment: int = 0
    total_images: int = 0
    
    # Output stats
    csv_rows_generated: int = 0
    
    # Errors
    errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)
    
    def add_error(self, error: str):
        """Add error message"""
        self.errors.append(error)
    
    def print_report(self):
        """Print final statistics report"""
        print("\n" + "=" * 80)
        print("PIPELINE COMPLETE")
        print("=" * 80)
        print(f"\n📊 STATISTICS:")
        print(f"  Input rows read:         {self.total_rows_read}")
        print(f"  Valid products parsed:   {self.valid_products}")
        print(f"  Product groups created:  {self.total_product_groups}")
        print(f"  Total variants:          {self.total_variants}")
        print(f"  Successfully processed:  {self.successfully_processed}/{self.total_product_groups}")
        print(f"  Failed URL searches:     {self.failed_url_search}")
        print(f"  Failed image extraction: {self.failed_image_extraction}")
        print(f"  Failed enrichment:       {self.failed_enrichment}")
        print(f"  Total images extracted:  {self.total_images}")
        print(f"  Shopify CSV rows:        {self.csv_rows_generated}")
        print(f"  Processing time:         {self.processing_time_sec:.1f}s")
        
        if self.errors:
            print(f"\n⚠️  ERRORS ({len(self.errors)}):")
            for error in self.errors[:10]:
                print(f"  - {error}")
            if len(self.errors) > 10:
                print(f"  ... and {len(self.errors) - 10} more")
        
        print("\n" + "=" * 80)
