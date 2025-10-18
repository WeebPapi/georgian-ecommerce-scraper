"""
Automated Product Scraper System for Wishlist App
Handles scraping products from multiple Georgian e-commerce sites
and syncing them to PostgreSQL database
"""

# Test for name changes : 2

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
            
            # Target 1: Find all potential product image tags
            # We look for common classes used by the CS-Cart platform (which wishlist.ge uses)
            product_images = soup.find_all(
                'img', 
                class_=lambda x: x and any(cls in str(x) for cls in ['ty-pict', 'cm-image', 'cloud-zoom', 'product-image', 'detailed'])
            )

            for img in product_images:
                # Prioritize lazy-loaded attributes (data-src, data-lazy) as they often hold the full-res URL
                src = img.get('data-src') or img.get('data-lazy') or img.get('src') 
                
                if src:
                    full_url = urljoin(product_url, src)
                    
                    # Basic filtering to exclude site elements, not product images
                    if not any(exclude in full_url.lower() for exclude in ['logo', 'banner', 'icon', 'payment', 'social', 'footer', 'header', 'spacer', 'no-image']):
                        
                        # Filter out known low-resolution directories/patterns
                        if not any(size_pattern in full_url.lower() for size_pattern in ['/50/', '/100/', '_thumb', 'icon']):
                            images.append(full_url)
            
            # Deduplicate and sort (from high-res to low-res if possible)
            images = list(dict.fromkeys(images))
            
            # Fallback: If no images found or all were filtered out, try again with less strict filtering
            if not images and product_images:
                for img in product_images:
                    src = img.get('data-src') or img.get('data-lazy') or img.get('src') 
                    if src and not full_url in images:
                        images.append(urljoin(product_url, src))
            
            # --- END REVISED IMAGE EXTRACTION LOGIC ---
            
            
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
        print(f"   main image: {product['main_image_url']}")
        if product['description']:
            print(f"   Description: {product['description'][:60]}...")


class DatabaseManager:
    """Handles all database operations with proper connection management"""
    
    def __init__(self, db_url: str, test_mode: bool = False):
        self.db_url = db_url
        self.pool = None
        self.test_mode = test_mode
        self.stats = {
            'successful_upserts': 0,
            'failed_upserts': 0,
            'timeouts': 0
        }
    
    async def connect(self):
        """Create database connection pool"""
        if self.test_mode:
            print("\n🧪 TEST MODE: Skipping database connection")
            return
        
        self.pool = await asyncpg.create_pool(
            self.db_url, 
            min_size=2, 
            max_size=20,  # Increased pool size for parallel operations
            command_timeout=30,  # 30 second timeout per command
            max_inactive_connection_lifetime=300  # Close idle connections after 5 min
        )
        logger.info("Database pool created")
        
        # Create index once on startup
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_products_external_source 
                    ON products(external_id, source)
                """)
                logger.info("Database indexes verified")
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
    
    async def close(self):
        """Close database connection pool with timeout"""
        if self.pool:
            try:
                # Print stats before closing
                if not self.test_mode:
                    print(f"\n📊 Database Stats:")
                    print(f"   ✅ Successful upserts: {self.stats['successful_upserts']}")
                    print(f"   ❌ Failed upserts: {self.stats['failed_upserts']}")
                    print(f"   ⏱️  Timeouts: {self.stats['timeouts']}")
                
                # Use asyncio.wait_for to prevent hanging
                await asyncio.wait_for(self.pool.close(), timeout=10.0)
                logger.info("Database pool closed successfully")
            except asyncio.TimeoutError:
                logger.warning("Pool closure timed out, terminating forcefully")
                self.pool.terminate()
                logger.info("Database pool terminated")
            except Exception as e:
                logger.error(f"Error closing pool: {e}")
                self.pool.terminate()
    
    async def upsert_product(self, product: Dict) -> bool:
        """Insert or update a product in the database with retry logic"""
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
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self.pool.acquire() as conn:
                    now = datetime.utcnow()
                    
                    # Use a transaction for safety
                    async with conn.transaction():
                        result = await asyncio.wait_for(
                            conn.fetchval(
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
                            ),
                            timeout=15.0  # 15 second timeout for this operation
                        )
                    
                    self.stats['successful_upserts'] += 1
                    return result is not None
                    
            except asyncio.TimeoutError:
                self.stats['timeouts'] += 1
                logger.warning(f"Timeout upserting product {product.get('name')} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)  # Wait before retry
                    continue
                self.stats['failed_upserts'] += 1
                return False
                
            except asyncpg.exceptions.UniqueViolationError:
                # Race condition - product already exists, this is actually success
                logger.debug(f"Product {product.get('external_id')} already exists")
                self.stats['successful_upserts'] += 1
                return True
                
            except Exception as e:
                logger.error(f"Error upserting product {product.get('name')}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                self.stats['failed_upserts'] += 1
                return False
        
        return False
    
    async def upsert_products_batch(self, products: List[Dict], batch_size: int = 50) -> int:
        """Batch upsert products for better performance"""
        if self.test_mode:
            print(f"   📝 [TEST MODE] Would batch upsert {len(products)} products")
            return len(products)
        
        successful = 0
        
        # Process in batches to avoid overwhelming the database
        for i in range(0, len(products), batch_size):
            batch = products[i:i + batch_size]
            print(f"   💾 Processing batch {i//batch_size + 1} ({len(batch)} products)...")
            
            # Process batch items concurrently but with limit
            tasks = [self.upsert_product(product) for product in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successes
            successful += sum(1 for r in results if r is True)
            
            # Small delay between batches to prevent overwhelming the pool
            if i + batch_size < len(products):
                await asyncio.sleep(0.5)
        
        print(f"   ✅ Successfully saved {successful}/{len(products)} products")
        return successful
    
    async def mark_products_as_unavailable(self, source: str, scraped_ids: List[str]):
        """Mark products not found in scrape as potentially unavailable"""
        if self.test_mode:
            print(f"   📝 [TEST MODE] Would mark unavailable products for source: {source}")
            return
        
        if not scraped_ids:
            logger.warning(f"No scraped IDs provided for {source}, skipping unavailable marking")
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
                result = await asyncio.wait_for(
                    conn.execute(
                        query,
                        datetime.utcnow(),
                        source,
                        scraped_ids,
                        cutoff_time
                    ),
                    timeout=30.0
                )
                logger.info(f"Marked unavailable products for {source}: {result}")
        except asyncio.TimeoutError:
            logger.error(f"Timeout marking unavailable products for {source}")
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
                await asyncio.wait_for(
                    conn.execute(
                        query,
                        log_data['source'],
                        log_data['products_found'],
                        log_data['products_updated'],
                        log_data['products_added'],
                        json.dumps(log_data.get('errors', [])),
                        log_data['started_at'],
                        log_data['completed_at'],
                        log_data['status']
                    ),
                    timeout=10.0
                )
        except asyncio.TimeoutError:
            logger.error(f"Timeout logging scrape for {log_data['source']}")
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
    
    async def scrape_all_sources(self):
        """Scrape all configured sources"""
        print("\n" + "="*60)
        print("🚀 STARTING SCRAPER")
        print("="*60)
        
        for source_name, scraper in self.scrapers.items():
            if not self.scrape_config.get(source_name):
                print(f"\n⏭️  Skipping {source_name} - no category URLs configured")
                continue
            
            await self.scrape_source(source_name, scraper)
    
    async def scrape_source(self, source_name: str, scraper: ProductScraper):
        """Scrape a single source with batch processing"""
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
                if isinstance(category_data, list):
                    category_url, category_name = category_data
                else:
                    category_url = category_data
                    category_name = 'general'
                
                products = scraper.scrape_category(category_url, category_name)
                all_products.extend(products)
                await asyncio.sleep(2)
            
            log_data['products_found'] = len(all_products)
            
            # Use BATCH upsert instead of individual operations
            print(f"\n💾 Saving {len(all_products)} products to database (using batch processing)...")
            
            # Collect external IDs
            for product in all_products:
                if product.get('external_id'):
                    scraped_external_ids.append(product['external_id'])
            
            # Batch upsert all products
            successful_count = await self.db.upsert_products_batch(all_products, batch_size=50)
            log_data['products_updated'] = successful_count
            
            # Mark products not found as unavailable
            if scraped_external_ids:
                await self.db.mark_products_as_unavailable(source_name, scraped_external_ids)
            
            log_data['status'] = 'completed'
            print(f"\n✅ Completed scraping {source_name}: {log_data['products_found']} found, {successful_count} saved")
            
        except Exception as e:
            logger.error(f"Error scraping {source_name}: {e}")
            log_data['errors'].append(str(e))
            log_data['status'] = 'failed'
            print(f"❌ Failed to scrape {source_name}: {e}")
        
        finally:
            log_data['completed_at'] = datetime.utcnow()
            await self.db.log_scrape(log_data)


async def run_scraper(test_mode: bool = False):
    """Main function to run the scraper with proper cleanup"""
    db_url = os.getenv('DATABASE_URL', 'postgresql://postgres.ndhuzmoyrvmwhiuqnyha:oVAvdosprwELCfFI@aws-1-eu-west-1.pooler.supabase.com:6543/postgres')
    
    db_manager = DatabaseManager(db_url, test_mode=test_mode)
    
    try:
        await db_manager.connect()
        orchestrator = ScraperOrchestrator(db_manager)
        await orchestrator.scrape_all_sources()
        
    except Exception as e:
        logger.error(f"Fatal error in scraper: {e}")
        print(f"\n❌ Fatal error: {e}")
    finally:
        await db_manager.close()
        print("\n" + "="*60)
        print("🏁 SCRAPER FINISHED")
        print("="*60)


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
                ['https://veli.store/category/sporti-mogzauroba/fan-shop/5332/', 'SportTravel'],
                ['https://veli.store/category/sakhlis-movla/308/', 'Housecare'],
                ['https://veli.store/category/ckhovelebis-movla/1376/', 'Petcare'],
                ['https://veli.store/category/bari-meti/1561/', 'Drinks'],
                ['https://veli.store/category/remonti-khelsatsyoebi/7196/', 'Construction'],
                ['https://veli.store/category/mshobeli-bavshvi/188/', 'Childcare'],
                ['https://veli.store/category/tsignebi/62/', 'Books'],
                ['https://veli.store/category/tansacmeli-aqsesuarebi/6263/', 'Clothes'],
                ['https://veli.store/category/satamashoebi/700/', 'Toys'],
                ['https://veli.store/category/avto-moto/2329/', 'Vehicle Accessories'],
                ['https://veli.store/category/sachuqrebi/2264/', 'Gifts'],
                ['https://veli.store/category/sakancelario-krafti/114/', 'School'],
                ['https://veli.store/category/yoveldghiuri-sayidlebi/5196/', 'General'],
            ],
            'extra': [],
            'wishlist': [
                ['https://wishlist.ge/%E1%83%A2%E1%83%94%E1%83%9A%E1%83%94%E1%83%A4%E1%83%9D%E1%83%9C%E1%83%94%E1%83%91%E1%83%98-%E1%83%A2%E1%83%90%E1%83%91%E1%83%94%E1%83%91%E1%83%98-%E1%83%90%E1%83%A5%E1%83%A1%E1%83%94%E1%83%A1%E1%83%A3%E1%83%90%E1%83%A0%E1%83%94%E1%83%91%E1%83%98/', 'Technology'],
                ['https://wishlist.ge/%E1%83%A1%E1%83%90%E1%83%9D%E1%83%A4%E1%83%98%E1%83%A1%E1%83%94-%E1%83%93%E1%83%90-%E1%83%A5%E1%83%A1%E1%83%94%E1%83%9A%E1%83%A3%E1%83%A0%E1%83%98-%E1%83%A2%E1%83%94%E1%83%A5%E1%83%9C%E1%83%98%E1%83%99%E1%83%90/', 'Technology'],
                ['https://wishlist.ge/%E1%83%9B%E1%83%A8%E1%83%94%E1%83%9C%E1%83%94%E1%83%91%E1%83%9A%E1%83%9D%E1%83%91%E1%83%90-%E1%83%A0%E1%83%94%E1%83%9B%E1%83%9D%E1%83%9C%E1%83%A2%E1%83%98/', 'Construction'],
                ['https://wishlist.ge/%E1%83%AC%E1%83%95%E1%83%A0%E1%83%98%E1%83%9A%E1%83%98-%E1%83%A1%E1%83%90%E1%83%A7%E1%83%9D%E1%83%A4%E1%83%90%E1%83%AA%E1%83%AE%E1%83%9D%E1%83%95%E1%83%A0%E1%83%94%E1%83%91%E1%83%9D-%E1%83%A2%E1%83%94%E1%83%A5%E1%83%9C%E1%83%98%E1%83%99%E1%83%90/', 'Household'],
                ['https://wishlist.ge/%E1%83%9B%E1%83%A1%E1%83%AE%E1%83%95%E1%83%98%E1%83%9A%E1%83%98-%E1%83%A1%E1%83%90%E1%83%A7%E1%83%9D%E1%83%A4%E1%83%90%E1%83%AA%E1%83%AE%E1%83%9D%E1%83%95%E1%83%A0%E1%83%94%E1%83%91%E1%83%9D-%E1%83%A2%E1%83%94%E1%83%A5%E1%83%9C%E1%83%98%E1%83%99%E1%83%90/', 'Household'],
                ['https://wishlist.ge/%E1%83%99%E1%83%9A%E1%83%98%E1%83%9B%E1%83%90%E1%83%A2%E1%83%A3%E1%83%A0%E1%83%98-%E1%83%A2%E1%83%94%E1%83%A5%E1%83%9C%E1%83%98%E1%83%99%E1%83%90/', 'Cooling'],
                ['https://wishlist.ge/%E1%83%9C%E1%83%9D%E1%83%A3%E1%83%97%E1%83%91%E1%83%A3%E1%83%A5%E1%83%94%E1%83%91%E1%83%98-%E1%83%99%E1%83%9D%E1%83%9B%E1%83%9E%E1%83%98%E1%83%A3%E1%83%A2%E1%83%94%E1%83%A0%E1%83%94%E1%83%91%E1%83%98-%E1%83%99%E1%83%9D%E1%83%9C%E1%83%A1%E1%83%9D%E1%83%9A%E1%83%94%E1%83%91%E1%83%98/', 'Computers'],
                ['https://wishlist.ge/%E1%83%A2%E1%83%94%E1%83%9A%E1%83%94%E1%83%95%E1%83%98%E1%83%96%E1%83%9D%E1%83%A0%E1%83%94%E1%83%91%E1%83%98-%E1%83%A4%E1%83%9D%E1%83%A2%E1%83%9D-%E1%83%95%E1%83%98%E1%83%93%E1%83%94%E1%83%9D-%E1%83%A2%E1%83%94%E1%83%A5%E1%83%9C%E1%83%98%E1%83%99%E1%83%90/', 'Media'],
                ['https://wishlist.ge/%E1%83%94%E1%83%96%E1%83%9D-%E1%83%93%E1%83%90-%E1%83%91%E1%83%90%E1%83%A6%E1%83%98/', 'Garden'],
                ['https://wishlist.ge/%E1%83%AD%E1%83%A3%E1%83%A0%E1%83%AD%E1%83%94%E1%83%9A%E1%83%98-%E1%83%93%E1%83%90-%E1%83%A1%E1%83%90%E1%83%9B%E1%83%96%E1%83%90%E1%83%A0%E1%83%94%E1%83%A3%E1%83%9A%E1%83%9D%E1%83%A1-%E1%83%90%E1%83%A5%E1%83%A1%E1%83%94%E1%83%A1%E1%83%A3%E1%83%90%E1%83%A0%E1%83%94%E1%83%91%E1%83%98/', 'Kitchen'],
                ['https://wishlist.ge/%E1%83%A1%E1%83%90%E1%83%96%E1%83%90%E1%83%A4%E1%83%AE%E1%83%A3%E1%83%9A%E1%83%9D-%E1%83%A8%E1%83%94%E1%83%9B%E1%83%9D%E1%83%97%E1%83%90%E1%83%95%E1%83%90%E1%83%96%E1%83%94%E1%83%91%E1%83%94%E1%83%91%E1%83%98-%E1%83%90%E1%83%A3%E1%83%96%E1%83%94%E1%83%91%E1%83%98-%E1%83%90%E1%83%A5%E1%83%A1%E1%83%94%E1%83%A1%E1%83%A3%E1%83%90%E1%83%A0%E1%83%94%E1%83%91%E1%83%98/', 'Pools-Pool_accessories'],
                ['https://wishlist.ge/%E1%83%91%E1%83%90%E1%83%95%E1%83%A8%E1%83%95%E1%83%97%E1%83%90-%E1%83%A1%E1%83%90%E1%83%9B%E1%83%A7%E1%83%90%E1%83%A0%E1%83%9D/%E1%83%94%E1%83%9A%E1%83%94%E1%83%A5%E1%83%A2%E1%83%A0%E1%83%9D-%E1%83%9B%E1%83%90%E1%83%9C%E1%83%A5%E1%83%90%E1%83%9C%E1%83%94%E1%83%91%E1%83%98/', 'Toys'],
                ['https://wishlist.ge/%E1%83%91%E1%83%90%E1%83%95%E1%83%A8%E1%83%95%E1%83%97%E1%83%90-%E1%83%A1%E1%83%90%E1%83%9B%E1%83%A7%E1%83%90%E1%83%A0%E1%83%9D/%E1%83%A1%E1%83%90%E1%83%91%E1%83%90%E1%83%95%E1%83%A8%E1%83%95%E1%83%9D-%E1%83%94%E1%83%A2%E1%83%9A%E1%83%94%E1%83%91%E1%83%98/', 'Childcare'],
                ['https://wishlist.ge/%E1%83%91%E1%83%90%E1%83%95%E1%83%A8%E1%83%95%E1%83%97%E1%83%90-%E1%83%A1%E1%83%90%E1%83%9B%E1%83%A7%E1%83%90%E1%83%A0%E1%83%9D/%E1%83%A9%E1%83%95%E1%83%98%E1%83%9A%E1%83%98%E1%83%A1-%E1%83%A2%E1%83%90%E1%83%9C%E1%83%A1%E1%83%90%E1%83%AA%E1%83%9B%E1%83%94%E1%83%9A%E1%83%98/', 'Kid Clothes'],
                ['https://wishlist.ge/%E1%83%91%E1%83%90%E1%83%95%E1%83%A8%E1%83%95%E1%83%97%E1%83%90-%E1%83%A1%E1%83%90%E1%83%9B%E1%83%A7%E1%83%90%E1%83%A0%E1%83%9D/%E1%83%A1%E1%83%9E%E1%83%9D%E1%83%A0%E1%83%A2%E1%83%A3%E1%83%9A%E1%83%98-%E1%83%98%E1%83%9C%E1%83%95%E1%83%94%E1%83%9C%E1%83%A2%E1%83%90%E1%83%A0%E1%83%98/page-2/', 'Sports kids'],
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