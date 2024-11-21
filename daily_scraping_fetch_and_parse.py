import os
import json
import uuid
import boto3
import random
import random
import logging
import hashlib
from time import sleep
from dotenv import load_dotenv
from datetime import datetime, timezone



# Load environment variables
load_dotenv()

# AWS S3 Configuration
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
bucket_name = os.environ.get("BUCKET_NAME")

# Configuration
retailer = "autozone"
BULK_SIZE = 1000
TEST_PROXY = os.environ.get("TEST_PROXY")
destination_set = f"{retailer}_urls_temp"



# Initialize the S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)



def upload_to_s3(sanitized_products):
    print(sanitized_products)
    if not sanitized_products:
        return  # Avoid uploading an empty list

    jsonl_content = "\n".join(json.dumps(product) for product in sanitized_products)
    content_hash = hashlib.md5(jsonl_content.encode('utf-8')).hexdigest()
    now = datetime.now(timezone.utc)
    file_name = f"{retailer}/{now.strftime('%Y/%m/%d')}/{content_hash}-{uuid.uuid4()}.jsonl"

    s3.put_object(Bucket=bucket_name, Key=file_name, Body=jsonl_content)
    logging.info(f"Uploaded {len(sanitized_products)} products to S3 as {file_name}")



# Function to fetch a web page
async def fetch_api(session, product_urls, proxy=None):
    # Up to 53 urls will be fetched in a single API call.
    api_url_template = 'https://www.autozone.com/ecomm/b2c/browse/v3/skus/price-availability/{sku_string}'
    skus = []
    for product_url in product_urls:
        sku = product_url.split('?')[0].split('/')[-1].replace('_0_0', '')
        skus.append(sku)
    sku_string = ','.join(skus)
    api_url = api_url_template.format(sku_string=sku_string)
    
    try:
        # if proxy:
        #     session.s.proxies = {"http": proxy, "https": proxy}
        
        # Reset the session after every 99 requests. This is important. Otherwise proxy will be blocked.
        if session.s_counter == 99:
            session.close()
            session.reset_session_callback(proxy)

        # Every once in a while, open the homepage. This adds some randomization to the requests pattern.
        if random.random() < 0.04: 
            logging.info('\nOpening home page...')
            sleep(2)
            session.s_counter += 1
            session.total_counter += 1
            response = session.s.get('https://www.autozone.com/', proxies=proxy, timeout=60)
            logging.info(f'Homepage Status: {response.status_code}')

        logging.info(f'\nTotal Requests: {session.total_counter}')
        logging.info(f'Session Requests: {session.s_counter}')
        session.s_counter += 1
        session.total_counter += 1
        resp = session.s.get(url=api_url, proxies=proxy, timeout=30)
        logging.info(f'API Status: {resp.status_code}\n')
        
        if resp.status_code == 200:
            resp_json = resp.json()
            return resp_json
        else:
            raise Exception('Invalid status code.')
    
    except Exception as e:
        logging.exception('ex: ')
        logging.info(f"Error fetching {api_url}: {e}")
        # Reset session on error.
        session.close()
        session.reset_session_callback(proxy)
        return 'Error'



# Function to parse a web page
def parse_json(product_urls, resp_json):
    products = []
    for product_json in resp_json:
        sku = product_json['skuPricingAndAvailability']['skuId']
        brand = product_json['skuPricingAndAvailability']['brandName']
        availability = product_json['skuPricingAndAvailability']['shipToHomeAvailable']
        price = product_json['skuPricingAndAvailability']['retailPrice']
        for i in product_urls:
            if sku in i:
                url = i
                break

        product_dict = {
            "sku": sku,
            "retailer": 'autozone.com',
            "product_url": url,
            "retailers_brand": brand,
            "price": float(price) if price else None,
            "in_stock": availability,
            "currency": "USD",
            #"title": part_name,
            #"images": [image_url] if image_url else [],
            #"retailers_mpn": part_number,
            #"retailers_upc": [upc] if upc else [],
            #"avg_rating": rating,
            #"number_of_reviews": int(review_count) if review_count else None
        }
        products.append(product_dict)
    
    return products




# Function to validate the product JSON
def validate_product(product):
    try:
        if not isinstance(product.get('retailer'), str) or not product['retailer'].strip():
            raise ValueError("Invalid 'retailer': Must be a non-empty string")
        if not isinstance(product.get('product_url'), str) or not product['product_url'].strip():
            raise ValueError("Invalid 'product_url': Must be a non-empty string")
        if not isinstance(product.get('retailers_brand'), str) or not product['retailers_brand'].strip():
            raise ValueError("Invalid 'retailers_brand': Must be a non-empty string")
        # if not isinstance(product.get('title'), str) or not product['title'].strip():
        #     raise ValueError("Invalid 'title': Must be a non-empty string")
        # if not isinstance(product.get('retailers_mpn'), str) or not product['retailers_mpn'].strip():
        #     raise ValueError("Invalid 'retailers_mpn': Must be a non-empty string")
        
        # Convert price to a float if it's a string with commas
        price = product.get('price')
        if isinstance(price, str):
            price = price.replace(',', '').strip()
            try:
                price = float(price)
            except ValueError:
                logging.exception('ex: ')
                raise ValueError("Invalid 'price': Must be a valid number")
        elif not isinstance(price, (float, int)) or price is None:
            raise ValueError("Invalid 'price': Must be a non-null number")
        
        product['price'] = price
    except ValueError as e:
        logging.exception('ex: ')
        logging.info(f"Validation failed: {e}")
        return False
    return True



# Function to scrape a web page and write data to S3 asynchronously
async def scraper(session, product_urls, product_buffer, redis_client, proxy=None, test_mode=False):
    #proxy = TEST_PROXY if test_mode else (random.choice(proxies) if proxies else None)
    resp_json = await fetch_api(session, product_urls, proxy)

    if resp_json == 'Error':
        logging.info(f'Error in fetch. Adding urls back to redis {destination_set}.')
        await redis_client.sadd(destination_set, *product_urls)
    elif resp_json:  # Has data.
        products = parse_json(product_urls, resp_json)
        for product in products:
            #logging.info(product)
            if test_mode:
                product_buffer.append(product)
                if len(product_buffer) >= BULK_SIZE:
                    product_buffer.clear()
            else:
                if validate_product(product):
                    product_buffer.append(product)
                    if len(product_buffer) >= BULK_SIZE:
                        upload_to_s3(product_buffer)
                        product_buffer.clear()
                else:
                    logging.info(f"\n\nFailed product: {product}\n\n   ")



async def get_urls(redis_client, key, count):
    urls = await redis_client.spop(key, count)
    return [url.decode('utf-8') for url in urls] if urls else []



# Function to load proxies from Redis
async def load_proxies(redis_client):
    # The script is made for a single proxy IP that is expected to autorotate every 3 minutes. It has not been tested with other forms of proxies.
    logging.info('Reading proxy from redis...')
    try:
        proxy = await redis_client.get(os.environ.get("PROXY_REMOTE_REDIS_KEY"))
        proxy = proxy.decode('utf-8')
        proxy ={
            'http': f'http://{proxy}',
            'https': f'http://{proxy}'
        }
        return proxy
    except Exception as e:
        logging.exception('ex: ')
        logging.info(f"Error loading proxies: {e}")
        return []
