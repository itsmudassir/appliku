# IMPORTANT CONSIDERATIONS:
#   The script is made for a single proxy IP that is expected to autorotate every 3 minutes. It has not been tested with other forms of proxies.
#   The script has been made for a single thread (to reduce compexity). Concurrency should not be increased beyond 1. Even with a single thread, it is expected to complete within 24 hours.
#   The batch size of 53 is based on the API limit. Do not increase it beyond 53.

import os
import sys
import random
import asyncio
import logging
from time import sleep
from pathlib import Path
from datetime import datetime
from curl_cffi import requests
from dotenv import load_dotenv
import redis.asyncio as aioredis
from logging.handlers import RotatingFileHandler
from daily_scraping_fetch_and_parse import scraper, upload_to_s3, get_urls, load_proxies



# Load environment variables
load_dotenv()

# Redis Configuration
DB_HOST_LOCAL = os.environ.get("DB_HOST_LOCAL") or "localhost"
DB_PORT_LOCAL = int(os.environ.get("DB_PORT_LOCAL", 6379))
DB_PASS_LOCAL = os.environ.get("DB_PASS_LOCAL")
REDIS_DB_NUMBER_LOCAL = int(os.environ.get("REDIS_DB_NUMBER_LOCAL", 0))
CONCURRENCY = 1 #DO NOT INCREASE. The script has only been made for a single thread.
URLS_PER_BATCH = 53  #53 is the api batch limit. DO NOT INCREASE.
batch_size = 10000

# Retailer Configuration
retailer = "autozone"
source_set = f"{retailer}_api_urls_master"
destination_set = f"{retailer}_urls_temp"
scraping_state_key = f"{retailer}_scraping_state"



class Session():
    # A global session to be used by all requests. This session maintains the cookies, and is reset periodically.
    def __init__(self, proxy):
        self.s = None  #Will hold the curl-cffi session.
        self.s_counter = 0
        self.total_counter = 0
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            #'cookie': 'AZ_APP_BANNER_SHOWN=true; REQUEST_ID=LkkRDP5QobR-wlWsuI5zO; JSESSIONID=2r-1YK9NcDklecOkijAHweWG3YcqzL3-TNfO2v9vqwJR4oHs0zlG!716575407; WWW-WS-ROUTE=ffffffff09c20e6a45525d5f4f58455e445a4a4216ce; mt.v=5.2125170791.1729619865852; preferedstore=6997; preferredStoreId=6997; eCookieId=c009a655-bb0e-4abf-8318-fcb8ccbe461a; TLTSID=f7f352d0afbc16548c0300e0ed6ab7dd; TLTUID=f7f352d0afbc16548c0300e0ed6ab7dd; az_bt_cl=hPp0YAbH2l1Jzlm4TTBcIe13jBcHmqlU8iudtFVEdK7P+dnVB4w4gV2yQ7ymG1Pi; akacd_default=2147483647~rv=26~id=37337e443bd3c706da6aed2e27abd4f8; _pxhd=cdea3a1da0ba91aa2ed4e562a4420caecbc9762af03a1e36a9c8ee6d87dd0116:240f46c8-909f-11ef-a28f-c2ed938961cc; profileId=287871918560; rewardsId=; RES_TRACKINGID=817853868636394; userVehicleCount=0; cartProductPartIds=; cartProductSkus=; cartProductTitles=; cartProductVendors=; cartUnitPrice=; cartCorePrice=; cartDiscountPriceList=; userType=3; nddMarket=N%2FA; nddHub=N%2FA; nddStore=N%2FA; availableRewardBalance=0; thrive_fp_uuid=AAAAAZK1YNS5c3nE7p_bWoSIpiBy-antR_p11gJ8YXHc8rOl32ebIeyG; thrive_lp_msec=1729619875001; thrive_lp=https%3A%2F%2Fwww.autozone.com%2F; thrive_60m_msec=1729619875001; rxVisitor=1729619876020KHT6IOT4MSB687E1AO68KGKQQE5NLAI5; AMCVS_0E3334FA53DA8C980A490D44%40AdobeOrg=1; IR_gbd=autozone.com; _elevaate_session_id=166a73b2-ce13-4343-94e8-9c073a370c52; pxcts=2b88f22c-909f-11ef-a018-40ab358b7760; _pxvid=240f46c8-909f-11ef-a28f-c2ed938961cc; IR_PI=2bce1304-909f-11ef-a91f-8996dfdf110b%7C1729619876509; _ga=GA1.1.730168212.1729619877; _fbp=fb.1.1729619877264.499776681226119006; _gcl_au=1.1.478037525.1729619878; s_ecid=MCMID%7C55037401063926493841985675377088411224; AMCV_0E3334FA53DA8C980A490D44%40AdobeOrg=1406116232%7CMCIDTS%7C20019%7CMCMID%7C55037401063926493841985675377088411224%7CMCAAMLH-1730224676%7C3%7CMCAAMB-1730224676%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1729627076s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C2.5.0; sc_lv_s=First%20Visit; s_vnum=1730401200271%26vn%3D1; s_invisit=true; s_p24=https%3A%2F%2Fwww.autozone.com%2F; s_cc=true; dtCookie=v_4_srv_10_sn_BIMUTC18T38VLBO8GTGHOSCI4CJLTLI2_perc_100000_ol_0_mul_1_app-3A533fc11017a2a54e_1_rcs-3Acss_0; dtSa=-; _pxdc=Human; prevPageType=ProductShelf; loginInteractionMethod=; OptanonAlertBoxClosed=2024-10-22T17:58:38.506Z; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Oct+22+2024+22%3A58%3A39+GMT%2B0500+(Pakistan+Standard+Time)&version=202405.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=bedb48cf-5872-4f79-9f0a-f48360a29b89&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0004%3A1%2CC0002%3A1&AwaitingReconsent=false; BVBRANDID=abee7c7f-59ed-43d8-be89-40b9d53a46ed; BVBRANDSID=124a7d0a-8a55-4da6-9fbe-a7541e530ace; _px2=eyJ1IjoiNDRkMDMzZjAtOTA5Zi0xMWVmLTg0ZjItNzllZDk3ODg3YjE1IiwidiI6IjI0MGY0NmM4LTkwOWYtMTFlZi1hMjhmLWMyZWQ5Mzg5NjFjYyIsInQiOjE3Mjk2MjAyMjI2ODYsImgiOiIxMTQyMGFjYWE5OWEwZjZiZjdiYTViZDk5YTAyZGU3MWI2ZTNiMmU4Zjk2MTQ0MDkzNjAzOGZhZGMwZjQ4NmRjIn0=; level1Category=Auto Parts; level2Category=Brakes and Traction Control; level3Category=Disc Brake System; s_pn=az%3Acatalog%3Abrakes-and-traction-control%3Adisc-brake-system%3Ashelf; s_tbe=1729619923722; IR_21176=1729619924103%7C0%7C1729619924103%7C%7C; s_ppvl=az%253Acatalog%253Abrakes-and-traction-control%253Adisc-brake-system%253Ashelf%2C61%2C61%2C5259%2C539%2C607%2C1366%2C768%2C1%2CL; QSI_HistorySession=https%3A%2F%2Fwww.autozone.com%2F~1729619878525%7Chttps%3A%2F%2Fwww.autozone.com%2Fbrakes-and-traction-control%2Fbrake-bleeder-screw~1729619926024; _y2=1%3AeyJjIjp7IjI1ODk3NiI6MjU1NjM1OTI4fX0%3D%3ANzc2NzgzNzc2%3A99; utag_main=v_id:0192b560d1a900196df7cbae4c9d0506f00190670086e$_sn:1$_se:3$_ss:0$_st:1729621730543$ses_id:1729619874220%3Bexp-session$_pn:2%3Bexp-session$vapi_domain:autozone.com$dc_visit:1$dc_event:3%3Bexp-session$dc_region:ap-east-1%3Bexp-session; _uetsid=2c1eab70909f11ef81d0811e86ad833a|1ys1wk6|2|fq8|0|1756; _uetvid=2c1ed050909f11ef8be6d3e881748f7b|15i2or|1729619926337|2|1|bat.bing.com/p/insights/c/e; _ga_GEFNYC68GY=GS1.1.1729619877.1.1.1729619934.3.0.0; s_ppv=az%253Acatalog%253Abrakes-and-traction-control%253Adisc-brake-system%253Ashelf%2C83%2C61%2C7720%2C1115%2C607%2C1366%2C768%2C1%2CP; RT="z=1&dm=www.autozone.com&si=903f9cd1-a975-4437-b3ce-3dd42ff66bb0&ss=m2kr0by7&sl=1&tt=7is&rl=1&nu=4uvsg4qf&cl=1qd3"; sc_nrv=1729619944251-New; sc_lv=1729619944251; s_sq=autozmobilefirstprod%3D%2526c.%2526a.%2526activitymap.%2526page%253Daz%25253Acatalog%25253Abrakes-and-traction-control%25253Adisc-brake-system%25253Ashelf%2526link%253D2%2526region%253DpaginationButton%2526pageIDType%253D1%2526.activitymap%2526.a%2526.c%2526pid%253Daz%25253Acatalog%25253Abrakes-and-traction-control%25253Adisc-brake-system%25253Ashelf%2526pidt%253D1%2526oid%253Dhttps%25253A%25252F%25252Fwww.autozone.com%25252Fbrakes-and-traction-control%25252Fbrake-bleeder-screw%25253FpageNumber%25253D2%2526ot%253DA; rxvt=1729621744271|1729619879030; dtPC=10$419915154_22h-vARLPDMBWCFQLTASLDTHDHLKBMIJRCAHD-0e0; _yi=1%3AeyJsaSI6eyJjIjowLCJjb2wiOjMyOTAwMzAxNTIsImNwZyI6MjU5MDQyLCJjcGkiOjEwMTk5MDkzNDcxMywic2MiOjEsInRzIjoxNzI5NjE5OTMzNjYwfSwic2UiOnsiYyI6MSwiZWMiOjEyLCJsYSI6MTcyOTYxOTk0MzUzOSwicCI6Mywic2MiOjIzfSwidSI6eyJpZCI6IjUyOTEzNWNhLWI2MTctNDY3Mi1hYWFhLTg2YmNmMDFmY2M2NCIsImZsIjoiMCJ9fQ%3D%3D%3ALTE5NjU3ODQwMA%3D%3D%3A99; prevUrlRouteValue=%2Fbrakes-and-traction-control%2Fbrake-bleeder-screw%3FpageNumber%3D2; redirect_url=%2Fbrakes-and-traction-control%2Fbrake-bleeder-screw%3FpageNumber%3D2',
            #'ecookieid': 'c009a655-bb0e-4abf-8318-fcb8ccbe461a',
            'priority': 'u=1, i',
            'referer': 'https://www.autozone.com/',
            'sales-channel': 'AZRMFMOB',
            #'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            #'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            #'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'x-requested-by': 'MF',
        }
        self.reset_session_callback(proxy)  # Initialize the first session.

    def reset_session_callback(self, proxy):
        # Will refresh session cookies.
        # Retry creating a session every 30 seconds if there is an error, until successful.
        while True:
            try:
                logging.info('\nCreating new session...')
                browser = random.choice(['chrome120','chrome123', 'chrome124'])
                self.s = requests.Session(impersonate=browser)
                self.s.headers.update(self.headers)
                sleep(5)
                self.s_counter = 1
                self.total_counter += 1
                response = self.s.get('https://www.autozone.com/', proxies=proxy, timeout=60)
                logging.info(f'Session status: {response.status_code}')
                if response.status_code != 200:
                    raise Exception('Error in creating session!')
                else:
                    break
            except Exception as e:
                logging.exception('ex: ')
                logging.info(f'exception: {e}')
                logging.info('Error in creating session!')
                logging.info('\nPausing for 30 seconds...')
                sleep(30)

    def close(self):
        self.s.close()

 



# Function to check and refill URLs from remote Redis if local Redis is empty and state is outdated
async def refill_urls_from_remote(redis_client_remote, redis_client_local):
    # Check if the local Redis destination_set is empty
    if await redis_client_local.scard(destination_set) == 0:
        # Check the last scraping date
        last_scraping_date_bytes = await redis_client_local.get(scraping_state_key)
        last_scraping_date_str = last_scraping_date_bytes.decode("utf-8") if last_scraping_date_bytes else None
        today_date_str = datetime.now().strftime("%Y-%m-%d")

        # Proceed only if the last scraping date is empty or older than today
        if not last_scraping_date_str or last_scraping_date_str < today_date_str:
            logging.info(f"Refilling URLs for {retailer} from remote Redis to local Redis (last scraping date: {last_scraping_date_str}).")
            try:
                cursor = 0
                while True:
                    cursor, urls = await redis_client_remote.sscan(source_set, cursor=cursor, count=batch_size)
                    if urls:
                        await redis_client_local.sadd(destination_set, *urls)
                        logging.info('Batch inserted.')
                    if cursor == 0:
                        break
                logging.info(f"Refilled URLs from remote Redis to local Redis.")
                # Update the scraping state with today's date after a successful refill
                await redis_client_local.set(scraping_state_key, today_date_str)
                logging.info(f"Updated scraping state to today's date: {today_date_str}")
            except Exception as e:
                logging.exception('ex: ')
                logging.info(f"Error refilling URLs: {e}")
        else:
            logging.info(f"Skipping refill: {retailer} scraping data is already up-to-date (last scraping date: {last_scraping_date_str}).")
    else:
        logging.info(f"Skipping refill: {destination_set} is not empty.")



async def worker(redis_client, key, session, proxy=None):
    product_buffer = []
    while True:
        product_urls = await get_urls(redis_client, key, URLS_PER_BATCH)
        print("urls loaded ",len(product_urls))
        if not product_urls:
            break
        
        # Pass the set of 53 urls to the scraper. All 53 will be fetched in a single API call.
        await scraper(session, product_urls, product_buffer, redis_client, proxy=proxy)

    if product_buffer:
        upload_to_s3(product_buffer)



async def main():
 
    start_time = datetime.now()
    logging.info(f'\nStart Time: {start_time}')

    redis_client_LOCAL = aioredis.Redis(
        host=DB_HOST_LOCAL,
        port=DB_PORT_LOCAL,
        password=DB_PASS_LOCAL,
        db=REDIS_DB_NUMBER_LOCAL
    )

    redis_client_REMOTE = aioredis.Redis(
        host=os.environ.get("DB_HOST_REMOTE"),
        port=int(os.environ.get("DB_PORT_REMOTE", 6379)),
        password=os.environ.get("DB_PASS_REMOTE"),
        db=int(os.environ.get("REDIS_DB_NUMBER_REMOTE", 0))
    )

    try:
        proxy = await load_proxies(redis_client_LOCAL) if os.environ.get("PROXY_ENABLED", "false").lower() == "true" else None
        await refill_urls_from_remote(redis_client_REMOTE, redis_client_LOCAL)


        #Initialize inital session.
        session = Session(proxy)

        workers = [
            asyncio.create_task(worker(redis_client_LOCAL, destination_set, session, proxy=proxy))
            for _ in range(CONCURRENCY)
        ]
        await asyncio.gather(*workers)

        end_time = datetime.now()
        logging.info(f'\nStart Time: {start_time}')
        logging.info(f'End Time: {end_time}')
    finally:
        await redis_client_LOCAL.close()
        await redis_client_REMOTE.close()



if __name__ == "__main__":
    asyncio.run(main())
