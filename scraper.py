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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
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
    
    def scrape_category(self, url: str, category_name: str = 'general') -> List[Dict]:
        """Override this method for each store"""
        raise NotImplementedError


class VeliStoreScraper(ProductScraper):
    """Scraper specifically for veli.store"""
    
    def __init__(self):
        super().__init__('veli')
        self.base_url = 'https://veli.store'
    
    def scrape_category(self, category_url: str, category_name: str = 'general') -> List[Dict]:
        """Scrape products from a Veli.store category"""
        products = []
        
        try:
            print(f"\n{'='*60}")
            print(f"📂 Scraping category: {category_name} - {category_url}")
            print(f"{'='*60}")
            
            response = self.session.get(category_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find product links (adjust selectors based on actual HTML)
            product_links = soup.find_all('a', href=lambda x: x and '/details/' in x)
            
            print(f"✅ Found {len(product_links)} product links")
            
            for idx, link in enumerate(product_links, 1):
                product_url = urljoin(self.base_url, link['href'])
                print(f"\n🔍 [{idx}/{len(product_links)}] Scraping: {product_url}")
                
                product_data = self.scrape_product_details(product_url, category_name)
                if product_data:
                    products.append(product_data)
                    self._print_product(product_data)
                else:
                    print(f"   ❌ Failed to scrape product")
                    
        except Exception as e:
            logger.error(f"Error scraping Veli category {category_url}: {e}")
            print(f"❌ ERROR: {e}")
        
        return products
    
    def scrape_product_details(self, product_url: str, category_name: str = 'general') -> Optional[Dict]:
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
                'description': desc_elem.get_text(strip=True)[:100] if desc_elem else '',
                'price': self._extract_price(price_elem.get_text() if price_elem else '0'),
                'currency': 'GEL',
                'original_url': product_url,
                'main_image_url': images[0] if images else None,
                'gallery_images': images,
                'in_stock': True,
                'low_stock': False,
                'category': category_name,
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
    
    def _print_product(self, product: Dict):
        """Pretty print product details"""
        print(f"✅ Product scraped successfully:")
        print(f"   Name: {product['name'][:60]}")
        print(f"   Price: {product['price']} {product['currency']}")
        print(f"   ID: {product['external_id']}")
        print(f"   Category: {product['category']}")
        print(f"   Images: {len(product['gallery_images'])} found")
        if product['description']:
            print(f"   Description: {product['description'][:60]}...")


class ExtraGeScraper(ProductScraper):
    """Scraper for extra.ge"""
    
    def __init__(self):
        super().__init__('extra')
        self.base_url = 'https://extra.ge'
    
    def scrape_category(self, category_url: str, category_name: str = 'general') -> List[Dict]:
        """Implement similar to VeliStoreScraper"""
        logger.info(f"Scraping Extra.ge category: {category_url}")
        print(f"⚠️  Extra.ge scraper not fully implemented yet")
        return []


class WishlistGeScraper(ProductScraper):
    """Scraper for wishlist.ge with updated selectors"""
    
    def __init__(self):
        super().__init__('wishlist')
        self.base_url = 'https://wishlist.ge'
    
    def scrape_category(self, category_url: str, category_name: str = 'general') -> List[Dict]:
        """Scrape products from a Wishlist.ge category"""
        products = []
        
        try:
            print(f"\n{'='*60}")
            print(f"📂 Scraping category: {category_name} - {category_url}")
            print(f"{'='*60}")
            
            # Increased timeout to 30s to mitigate read timeouts
            response = self.session.get(category_url, timeout=30) 
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # --- UPDATED: Use the confirmed product container class ---
            print(f"Looking for products with class 'ty-grid-list__item_body'...")
            
            # Find product containers using the exact class from the screenshot
            product_containers = soup.find_all(
                'div', 
                # Look for the grid item, the list item, or the main column
                class_=lambda x: x and ('ty-grid-list__item' in x or 'ty-product-list__item' in x or 'ty-column' in x)
            )
            print(f"   ✓ Found {len(product_containers)} product containers")
            
            # Extract links from containers
            product_links = []
            for container in product_containers:
                # --- UPDATED: Target the reliable 'product-title' link ---
                link = container.find('a', class_='product-title')
                if not link:
                    # Fallback to the image link
                    link = container.find('a', class_='abt-single-image')
                
                if link and link.get('href'):
                    product_links.append(link)
            
            # Ensure links are unique
            unique_links = list({link['href']: link for link in product_links}.values())
            
            print(f"✅ Found {len(unique_links)} unique product links")
            
            if unique_links:
                print(f"   Sample URLs:")
                for i, link in enumerate(unique_links[:3], 1):
                    href = link.get('href', '')
                    print(f"      {i}. {href}")
            
            for idx, link in enumerate(unique_links, 1):
                if not link or not link.get('href'):
                    continue
                    
                product_url = urljoin(self.base_url, link['href'])
                print(f"\n🔍 [{idx}/{len(unique_links)}] Scraping: {product_url}")
                
                product_data = self.scrape_product_details(product_url, category_name)
                if product_data:
                    products.append(product_data)
                    self._print_product(product_data)
                else:
                    print(f"   ❌ Failed to scrape product")
                    
        except Exception as e:
            logger.error(f"Error scraping Wishlist.ge category {category_url}: {e}")
            print(f"❌ ERROR: {e}")
        
        return products
    
    def scrape_product_details(self, product_url: str, category_name: str = 'general') -> Optional[Dict]:
        """Scrape detailed product information from Wishlist.ge"""
        try:
            response = self.session.get(product_url, timeout=30) # Increased timeout
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # --- UPDATED: Extract ID from hidden input first (most reliable) ---
            external_id = None
            external_id_input = soup.find('input', {'name': 'product_id', 'type': 'hidden'})
            
            if external_id_input:
                external_id = external_id_input.get('value')
            elif 'product_id=' in product_url:
                external_id = product_url.split('product_id=')[-1].split('&')[0]
            else:
                # Fallback: Hash the URL for a unique ID
                parts = product_url.rstrip('/').split('/')
                external_id = parts[-1] if parts else f"wishlist-{hash(product_url)}"
            
            # Product name - Prioritized reliable selectors
            name_elem = (
                soup.find('h1', class_='ty-product-block-title') or
                soup.find('h1', {'itemprop': 'name'}) or
                soup.find('h1')
            )
            
            # Price - Look for ty-price-num or the actual price block
            price_elem = (
                soup.find('span', class_='ty-price-num') or
                soup.find('div', class_='ty-product-block__price-actual') or
                soup.find('span', {'itemprop': 'price'})
            )
            
            # Description - Look for description tab content ID
            desc_elem = (
                soup.find('div', id='content_description') or
                soup.find('div', class_='ty-product-block__description') or
                soup.find('div', class_='ty-wysiwyg-content')
            )
            
            # Extract images
            images = []
            
            # Method 1: Look for the main detailed image by ID pattern
            main_img = soup.find('img', id=lambda x: x and x.startswith('det_img'))
            if main_img:
                src = main_img.get('src') or main_img.get('data-src')
                if src:
                    images.append(urljoin(product_url, src))
            
            # Method 2: Look for all images in the gallery or with common product classes
            img_elements = soup.find_all('img', class_=lambda x: x and ('cm-image' in x or 'ty-pict' in x))
            for img in img_elements:
                src = img.get('src') or img.get('data-src') or img.get('data-lazy')
                if src and not any(x in src.lower() for x in ['logo', 'icon', 'placeholder', 'payment', 'button']):
                    full_url = urljoin(product_url, src)
                    if full_url not in images:
                        images.append(full_url)
            
            product = {
                'external_id': external_id,
                'source': self.source,
                'name': name_elem.get_text(strip=True) if name_elem else 'Unknown Product',
                'description': desc_elem.get_text(strip=True)[:500] if desc_elem else '',
                'price': self._extract_price(price_elem.get_text() if price_elem else '0'),
                'currency': 'GEL',
                'original_url': product_url,
                'main_image_url': images[0] if images else None,
                'gallery_images': images,
                'in_stock': self._check_stock(soup),
                'low_stock': False,
                'category': category_name,
                'metadata': {}
            }
            
            return product
            
        except Exception as e:
            logger.error(f"Error scraping Wishlist.ge product {product_url}: {e}")
            print(f"   ❌ Error: {e}")
            return None
    
    def _extract_price(self, price_text: str) -> float:
        """Extract numeric price from text"""
        import re
        price_text = price_text.replace('₾', '').replace('GEL', '').replace('gel', '')
        match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
        return float(match.group()) if match else 0.0
    
    def _extract_category_from_url(self, url: str) -> str:
        """Extract category from URL"""
        if '/category/' in url:
            parts = url.split('/category/')[1].split('/')
            return parts[0] if parts else 'general'
        elif '/cat/' in url:
            parts = url.split('/cat/')[1].split('/')
            return parts[0] if parts else 'general'
        return 'general'
    
    def _check_stock(self, soup) -> bool:
        """Check if product is in stock"""
        out_of_stock_text = ['out of stock', 'არ არის მარაგში', 'sold out', 'unavailable']
        page_text = soup.get_text().lower()
        
        for text in out_of_stock_text:
            if text in page_text:
                return False
        
        add_to_cart = soup.find(['button', 'a'], text=lambda x: x and 'cart' in str(x).lower())
        if add_to_cart and add_to_cart.get('disabled'):
            return False
        
        return True
    
    def _print_product(self, product: Dict):
        """Pretty print product details"""
        print(f"✅ Product scraped successfully:")
        print(f"   Name: {product['name'][:60]}")
        print(f"   Price: {product['price']} {product['currency']}")
        print(f"   ID: {product['external_id']}")
        print(f"   Category: {product['category']}")
        print(f"   Images: {len(product['gallery_images'])} found")
        print(f"   In Stock: {'Yes' if product['in_stock'] else 'No'}")
        if product['description']:
            print(f"   Description: {product['description'][:60]}...")


class DatabaseManager:
    """Handles all database operations"""
    
    def __init__(self, db_url: str, test_mode: bool = False):
        self.db_url = db_url
        self.pool = None
        self.test_mode = test_mode
    
    async def connect(self):
        """Create database connection pool"""
        if self.test_mode:
            print("\n🧪 TEST MODE: Skipping database connection")
            return
        
        self.pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=10)
        logger.info("Database pool created")
    
    async def close(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")
    
    async def upsert_product(self, product: Dict) -> bool:
        """Insert or update a product in the database"""
        if self.test_mode:
            print(f"   📝 [TEST MODE] Would upsert product: {product['name'][:50]}")
            return True
        
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
        if self.test_mode:
            print(f"   📝 [TEST MODE] Would mark unavailable products for source: {source}")
            return
        
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
        if self.test_mode:
            print(f"\n{'='*60}")
            print(f"📊 SCRAPE SUMMARY for {log_data['source'].upper()}")
            print(f"{'='*60}")
            print(f"Products found: {log_data['products_found']}")
            print(f"Products updated: {log_data['products_updated']}")
            print(f"Duration: {(log_data['completed_at'] - log_data['started_at']).seconds}s")
            print(f"Status: {log_data['status']}")
            if log_data.get('errors'):
                print(f"Errors: {len(log_data['errors'])}")
            return
        
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
        # Format: [URL, Category Name]
        self.scrape_config = {
            'veli': [
                ['https://veli.store/category/teqnika/774/', 'Technology'],
                ['https://veli.store/category/silamaze-movla/175/', 'Beauty'],
                ['https://veli.store/category/sporti-mogzauroba/fan-shop/5332/', 'Sport-Travel'],
            ],
            'extra': [],
            'wishlist': [
                ['https://wishlist.ge/%E1%83%A2%E1%83%94%E1%83%9A%E1%83%94%E1%83%A4%E1%83%9D%E1%83%9C%E1%83%94%E1%83%91%E1%83%98-%E1%83%A2%E1%83%90%E1%83%91%E1%83%94%E1%83%91%E1%83%98-%E1%83%90%E1%83%A5%E1%83%A1%E1%83%94%E1%83%A1%E1%83%A3%E1%83%90%E1%83%A0%E1%83%94%E1%83%91%E1%83%98/', 'Phones-Tablets'],
                ['https://wishlist.ge/%E1%83%A1%E1%83%90%E1%83%9D%E1%83%A4%E1%83%98%E1%83%A1%E1%83%94-%E1%83%93%E1%83%90-%E1%83%A5%E1%83%A1%E1%83%94%E1%83%9A%E1%83%A3%E1%83%A0%E1%83%98-%E1%83%A2%E1%83%94%E1%83%A5%E1%83%9C%E1%83%98%E1%83%99%E1%83%90/', 'Office-Tech'],
            ]
        }
    
    async def scrape_all_sources(self):
        """Scrape all configured sources"""
        print("\n" + "="*60)
        print("🚀 STARTING SCRAPER")
        print("="*60)
        
        for source_name, scraper in self.scrapers.items():
            # Skip sources with no configured URLs
            if not self.scrape_config.get(source_name):
                print(f"\n⏭️  Skipping {source_name} - no category URLs configured")
                continue
            
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
            print(f"\n🌐 Starting to scrape {source_name.upper()}...")
            all_products = []
            scraped_external_ids = []
            
            # Scrape all configured categories
            for category_data in self.scrape_config.get(source_name, []):
                # Extract URL and category name
                if isinstance(category_data, list):
                    category_url, category_name = category_data
                else:
                    # Fallback for old format (just URL strings)
                    category_url = category_data
                    category_name = 'general'
                
                products = scraper.scrape_category(category_url, category_name)
                all_products.extend(products)
                await asyncio.sleep(2)
            
            log_data['products_found'] = len(all_products)
            
            # Upsert products to database
            print(f"\n💾 Saving {len(all_products)} products to database...")
            for product in all_products:
                if product.get('external_id'):
                    scraped_external_ids.append(product['external_id'])
                    success = await self.db.upsert_product(product)
                    if success:
                        log_data['products_updated'] += 1
                    await asyncio.sleep(0.1)
            
            # Mark products not found as unavailable
            await self.db.mark_products_as_unavailable(source_name, scraped_external_ids)
            
            log_data['status'] = 'completed'
            print(f"\n✅ Completed scraping {source_name}: {log_data['products_found']} products found")
            
        except Exception as e:
            logger.error(f"Error scraping {source_name}: {e}")
            log_data['errors'].append(str(e))
            log_data['status'] = 'failed'
            print(f"❌ Failed to scrape {source_name}: {e}")
        
        finally:
            log_data['completed_at'] = datetime.utcnow()
            await self.db.log_scrape(log_data)


async def run_scraper(test_mode: bool = False):
    """Main function to run the scraper"""
    db_url = os.getenv('DATABASE_URL', 'postgresql://postgres.ndhuzmoyrvmwhiuqnyha:oVAvdosprwELCfFI@aws-1-eu-west-1.pooler.supabase.com:6543/postgres')
    
    # Initialize database manager
    db_manager = DatabaseManager(db_url, test_mode=test_mode)
    await db_manager.connect()
    
    try:
        # Create orchestrator and run scraper
        orchestrator = ScraperOrchestrator(db_manager)
        await orchestrator.scrape_all_sources()
        
    finally:
        await db_manager.close()
        print("\n" + "="*60)
        print("🏁 SCRAPER FINISHED")
        print("="*60)


if __name__ == "__main__":
    # Run in TEST MODE (no database operations)
    TEST_MODE = os.getenv('TEST_MODE', 'false').lower() == 'true'
    
    print(f"🔧 Running in {'TEST' if TEST_MODE else 'PRODUCTION'} mode")
    asyncio.run(run_scraper(test_mode=TEST_MODE))