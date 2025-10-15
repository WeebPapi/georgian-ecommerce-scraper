"""
Automated Product Scraper System for Wishlist App
Handles scraping products from multiple Georgian e-commerce sites
and syncing them to PostgreSQL database
"""

import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import os
from typing import List, Dict, Optional
from urllib.parse import urljoin
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProductScraper:
    """Base scraper class for all e-commerce sites"""
    
    def __init__(self, source_name: str):
        self.source = source_name
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def scrape_category(self, url: str) -> List[Dict]:
        """Override this method for each store"""
        raise NotImplementedError


class VeliStoreScraper(ProductScraper):
    """Scraper specifically for veli.store"""
    
    def __init__(self):
        super().__init__('veli')
        self.base_url = 'https://veli.store'
    
    def scrape_category(self, category_url: str) -> List[Dict]:
        """Scrape products from a Veli.store category"""
        products = []
        
        try:
            response = self.session.get(category_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find product cards (adjust selectors based on actual HTML)
            product_links = soup.find_all('a', href=lambda x: x and '/details/' in x)
            
            for link in product_links:
                product_url = urljoin(self.base_url, link['href'])
                product_data = self.scrape_product_details(product_url)
                if product_data:
                    products.append(product_data)
                    
        except Exception as e:
            logger.error(f"Error scraping Veli category {category_url}: {e}")
        
        return products
    
    def scrape_product_details(self, product_url: str) -> Optional[Dict]:
        """Scrape detailed product information"""
        try:
            response = self.session.get(product_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract SKU from URL
            external_id = product_url.split('sku=')[-1] if 'sku=' in product_url else None
            
            # Extract product data
            name_elem = soup.find('h1')
            price_elem = soup.find(['span', 'div'], class_=lambda x: x and 'price' in str(x).lower())
            desc_elem = soup.find(['div'], class_=lambda x: x and 'desc' in str(x).lower())
            
            # Extract images
            images = []
            for img in soup.find_all('img'):
                src = img.get('src') or img.get('data-src')
                if src and ('product' in src.lower() or 'upload' in src.lower()):
                    full_url = urljoin(product_url, src)
                    images.append(full_url)
            
            product = {
                'external_id': external_id,
                'source': self.source,
                'name': name_elem.get_text(strip=True) if name_elem else 'Unknown',
                'description': desc_elem.get_text(strip=True) if desc_elem else '',
                'price': self._extract_price(price_elem.get_text() if price_elem else '0'),
                'currency': 'GEL',
                'original_url': product_url,
                'main_image_url': images[0] if images else None,
                'gallery_images': images,
                'in_stock': True,  # Default, can add stock detection
                'low_stock': False,
                'category': self._extract_category_from_url(product_url),
                'metadata': {}
            }
            
            return product
            
        except Exception as e:
            logger.error(f"Error scraping product {product_url}: {e}")
            return None
    
    def _extract_price(self, price_text: str) -> float:
        """Extract numeric price from text"""
        import re
        match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
        return float(match.group()) if match else 0.0
    
    def _extract_category_from_url(self, url: str) -> str:
        """Extract category from URL"""
        if '/category/' in url:
            parts = url.split('/category/')[1].split('/')
            return parts[0] if parts else 'general'
        return 'general'


class ExtraGeScraper(ProductScraper):
    """Scraper for extra.ge"""
    
    def __init__(self):
        super().__init__('extra')
        self.base_url = 'https://extra.ge'
    
    def scrape_category(self, category_url: str) -> List[Dict]:
        """Implement similar to VeliStoreScraper"""
        # Implementation similar to Veli but with extra.ge specific selectors
        logger.info(f"Scraping Extra.ge category: {category_url}")
        return []  # Placeholder


class WishlistGeScraper(ProductScraper):
    """Scraper for wishlist.ge"""
    
    def __init__(self):
        super().__init__('wishlist')
        self.base_url = 'https://wishlist.ge'
    
    def scrape_category(self, category_url: str) -> List[Dict]:
        """Implement similar to VeliStoreScraper"""
        # Implementation similar to Veli but with wishlist.ge specific selectors
        logger.info(f"Scraping Wishlist.ge category: {category_url}")
        return []  # Placeholder


class DatabaseManager:
    """Handles all database operations"""
    
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.pool = None
    
    async def connect(self):
        """Create database connection pool"""
        self.pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=10)
        logger.info("Database pool created")
    
    async def close(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")
    
    async def upsert_product(self, product: Dict) -> bool:
        """Insert or update a product in the database"""
        query = """
            INSERT INTO products (
                external_id, source, name, description, price, currency,
                original_url, main_image_url, gallery_images, category,
                in_stock, low_stock, metadata, last_scraped_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (external_id, source) 
            DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                price = EXCLUDED.price,
                original_url = EXCLUDED.original_url,
                main_image_url = EXCLUDED.main_image_url,
                gallery_images = EXCLUDED.gallery_images,
                category = EXCLUDED.category,
                in_stock = EXCLUDED.in_stock,
                low_stock = EXCLUDED.low_stock,
                metadata = EXCLUDED.metadata,
                last_scraped_at = EXCLUDED.last_scraped_at,
                updated_at = EXCLUDED.updated_at
            RETURNING id
        """
        
        try:
            async with self.pool.acquire() as conn:
                # First, add unique constraint if not exists
                await conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_products_external_source 
                    ON products(external_id, source)
                """)
                
                now = datetime.utcnow()
                result = await conn.fetchval(
                    query,
                    product['external_id'],
                    product['source'],
                    product['name'],
                    product['description'],
                    product['price'],
                    product['currency'],
                    product['original_url'],
                    product['main_image_url'],
                    json.dumps(product['gallery_images']),
                    product['category'],
                    product['in_stock'],
                    product['low_stock'],
                    json.dumps(product['metadata']),
                    now,
                    now
                )
                return result is not None
        except Exception as e:
            logger.error(f"Error upserting product {product.get('name')}: {e}")
            return False
    
    async def mark_products_as_unavailable(self, source: str, scraped_ids: List[str]):
        """Mark products not found in scrape as potentially unavailable"""
        query = """
            UPDATE products 
            SET in_stock = false, updated_at = $1
            WHERE source = $2 
            AND external_id NOT IN (SELECT unnest($3::text[]))
            AND last_scraped_at < $4
        """
        
        try:
            async with self.pool.acquire() as conn:
                cutoff_time = datetime.utcnow() - timedelta(hours=48)
                await conn.execute(
                    query,
                    datetime.utcnow(),
                    source,
                    scraped_ids,
                    cutoff_time
                )
        except Exception as e:
            logger.error(f"Error marking unavailable products: {e}")
    
    async def log_scrape(self, log_data: Dict):
        """Log scraper run results"""
        query = """
            INSERT INTO scraper_logs (
                source, products_found, products_updated, products_added,
                errors, started_at, completed_at, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
        
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    query,
                    log_data['source'],
                    log_data['products_found'],
                    log_data['products_updated'],
                    log_data['products_added'],
                    json.dumps(log_data.get('errors', [])),
                    log_data['started_at'],
                    log_data['completed_at'],
                    log_data['status']
                )
        except Exception as e:
            logger.error(f"Error logging scrape: {e}")


class ScraperOrchestrator:
    """Orchestrates scraping from all sources"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.scrapers = {
            'veli': VeliStoreScraper(),
            'extra': ExtraGeScraper(),
            'wishlist': WishlistGeScraper()
        }
        
        # Define categories to scrape from each source
        self.scrape_config = {
            'veli': [
                'https://veli.store/category/teqnika/774/',
                'https://veli.store/category/silamaze-movla/175/',
                'https://veli.store/category/sporti-mogzauroba/fan-shop/5332/',
            ],
            'extra': [
                # Add Extra.ge category URLs
            ],
            'wishlist': [
                # Add Wishlist.ge category URLs
            ]
        }
    
    async def scrape_all_sources(self):
        """Scrape all configured sources"""
        logger.info("Starting full scrape of all sources")
        
        for source_name, scraper in self.scrapers.items():
            if source_name == 'veli':  # Only Veli is implemented for now
                await self.scrape_source(source_name, scraper)
    
    async def scrape_source(self, source_name: str, scraper: ProductScraper):
        """Scrape a single source"""
        log_data = {
            'source': source_name,
            'products_found': 0,
            'products_updated': 0,
            'products_added': 0,
            'errors': [],
            'started_at': datetime.utcnow(),
            'completed_at': None,
            'status': 'running'
        }
        
        try:
            logger.info(f"Scraping {source_name}...")
            all_products = []
            scraped_external_ids = []
            
            # Scrape all configured categories
            for category_url in self.scrape_config.get(source_name, []):
                logger.info(f"Scraping category: {category_url}")
                products = scraper.scrape_category(category_url)
                all_products.extend(products)
                
                # Rate limiting - be respectful
                await asyncio.sleep(2)
            
            log_data['products_found'] = len(all_products)
            
            # Upsert products to database
            for product in all_products:
                if product.get('external_id'):
                    scraped_external_ids.append(product['external_id'])
                    success = await self.db.upsert_product(product)
                    if success:
                        log_data['products_updated'] += 1
                    await asyncio.sleep(0.1)  # Small delay between DB ops
            
            # Mark products not found as unavailable
            await self.db.mark_products_as_unavailable(source_name, scraped_external_ids)
            
            log_data['status'] = 'completed'
            logger.info(f"Completed scraping {source_name}: {log_data['products_found']} products found")
            
        except Exception as e:
            logger.error(f"Error scraping {source_name}: {e}")
            log_data['errors'].append(str(e))
            log_data['status'] = 'failed'
        
        finally:
            log_data['completed_at'] = datetime.utcnow()
            await self.db.log_scrape(log_data)


async def run_scraper():
    """Main function to run the scraper"""
    # Get database URL from environment
    db_url = os.getenv('DATABASE_URL', 'postgresql://user:pass@localhost:5432/wishlist_db')
    
    # Initialize database manager
    db_manager = DatabaseManager(db_url)
    await db_manager.connect()
    
    try:
        # Create orchestrator and run scraper
        orchestrator = ScraperOrchestrator(db_manager)
        await orchestrator.scrape_all_sources()
        
    finally:
        await db_manager.close()


if __name__ == "__main__":
    # Run the scraper
    asyncio.run(run_scraper())