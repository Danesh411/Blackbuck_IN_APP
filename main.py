import json
# from curl_cffi import requests
import requests
import pydash as _
import pandas as pd
from pathlib import Path
from parsel import Selector
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

url = "https://loadboard-api-gateway.blackbuck.com/supplywrapper/bookload/v5/getIndentList"

headers = {
    "Accept-Encoding": "gzip",
    "Accept-Language": "en",
    "Android-Version": "28",
    "App-Version": "1100",
    "Authorization": "Token eyJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3NjIyNDQxMjMsInJhbmRvbWl6ZXIiOiIyOTMyMzBlYS0wMjQxLTQxMmEtODhiNy00M2MxMjQzYTY4ZTgiLCJ1c2VyQ3JlZGVudGlhbHNJZCI6IjY5MDliNjFiMzdlZTU2MGUzMzg4Mzc0MSIsInRlbmFudCI6IlNVUFBMWSIsImNsaWVudCI6IkJPU1NfQVBQIiwib2xkQ2xpZW50SWQiOjI3LCJ0dGwiOjUxODQwMDAwMDAsImlzR3Vlc3RVc2VyIjpmYWxzZSwic2Vzc2lvblR5cGUiOiJOT05fTUlNSUMiLCJpc3MiOiJBVVRIRU5USUNBVElPTiJ9.hp6U1PJ05ITtp1PnxF7-w0CSw8moEPKOibzro2713wk",
    "Connection": "Keep-Alive",
    "Content-Type": "application/json; charset=UTF-8",
    "Device-UUID": "",
    "Host": "loadboard-api-gateway.blackbuck.com",
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; SM-N976N Build/QP1A.190711.020)",
    "x-aaa-enabled": "true",
    "X-B3-Sampled": "1",
    "X-B3-SpanId": "9f9c55ba602a4fc8",
    "X-B3-TraceId": "9f9c55ba602a4fc8",
    "X-Consumer-Tenant-Id": "boss",
    "x-device-id": "e876569ff50b0d48",
    "X-Forwarded-For": "172.16.6.15",
    "x-frb-check-token": "",
    "X-SOURCE-ID": "com.zinka.boss"
}

df = pd.read_excel("Feasibility_check_for_Blackbuck_application_inputfile_2025_11_03_21_52.xlsx", nrows=10)
final_result = []

def coordinate_extractor(address):
    headers1 = {
        'appid': '2',
        'channel-entity': 'customer',
        'authorization': 'Bearer',
        'sec-ch-ua-platform': '"Windows"',
        'Referer': 'https://m.rapido.bike/unup-home/seo/Makarba,%20Ahmedabad,%20Gujarat,%20India/Shivranjani%20Cross%20Road,%20Jodhpur%20Village,%20Ahmedabad,%20Gujarat,%20India?version=v3',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'channel-name': 'pwa',
        'channel-host': 'browser',
        'sec-ch-ua-mobile': '?0',
        'appversion': '214',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json',
    }
    json_data = {
        'searchWord': address,
    }
    response = requests.post('https://m.rapido.bike/pwa/api/unup/autocomplete/location', headers=headers1, json=json_data)
    place_id = _.get(response.json(),'data[0].placeId',"N/A")
    return place_id

def process_row(row):
    # fetch_id = row["ID"]
    source = row["Start destination"]
    destination = row["End destination"]
    source_placeid = coordinate_extractor(source)
    destination_placeid = coordinate_extractor(destination)

    payload = {
  "call_logs_permission_enabled": False,
  "draw_over_apps_permission_enabled": False,
  "from_google_place_id": source_placeid,
  "from_location_coordinates": None,
  "from_location_name": source,
  "from_zone_id": None,
  "indent_id": None,
  "is_brokerage_only_loads": False,
  "sp_mobile": "9726149068",
  "offset": 0,
  "page": 1,
  "size": 10,
  "recommended_lanes_type": "LF",
  "screen_type": "SRP",
  "search_session_logging_id": None,
  # "date": 1762244129371,
  "date": 1762321079274,
  "to_google_place_id": destination_placeid,
  "to_location_coordinates": None,
  "to_location_name": destination,
  "to_zone_id": None,
  "truck_attribute_list": None,
  "truck_number": None
}
    proxy = {
        "http": 'http://f42a5b59aec3467e97a8794c611c436b91589634343:super=false@proxy.scrape.do:8080',
        "https": 'http://f42a5b59aec3467e97a8794c611c436b91589634343:super=false@proxy.scrape.do:8080',
    }

    response = requests.post(url,
                             headers=headers,
                             data=json.dumps(payload),
                             proxies=proxy,
                             verify=False
                             # impersonate="chrome120",
                             )

    for sub_json in  _.get(response.json(),"load_details",[]):
        instance_json = _.get(sub_json,"indent",{})
        customer_details = _.get(instance_json,"customer_details",{})
        truck_details = _.get(instance_json,"truck_type_requirement_list[0]","N/A")

        distance = _.get(instance_json,'distance',"N/A")
        price_per_truck = _.get(instance_json,'truck_type_details_value',"N/A")
        to_location = _.get(instance_json, 'to_location.name', "N/A")
        from_location = _.get(instance_json, 'from_location.name', "N/A")
        product_type = _.get(instance_json, 'product_type', "N/A")

        customer_id = _.get(customer_details,'customer_id',"N/A")
        customer_business_name = _.get(customer_details,'business_name',"N/A")
        customer_profile_type = _.get(customer_details,'profile_type',"N/A")
        customer_verification_status = _.get(customer_details,'verification_status',"N/A")
        customer_proprietor_name = _.get(customer_details,'proprietor_name',"N/A")
        customer_review_count = _.get(customer_details,'customer_rating.total_rating_count',"N/A")
        customer_rating = _.get(customer_details,'customer_rating.total_rating',"N/A")
        customer_location = _.get(customer_details,'customer_location',"N/A")
        customer_contact = _.get(customer_details,'contact_list',"N/A")


        truck_type = _.get(truck_details,'body_type_group_text',"N/A")
        try:
            truck_size_check = _.get(truck_details,'truck_length_range_text',"N/A")
            truck_size_selector = Selector(text=truck_size_check)
            truck_size_content = truck_size_selector.xpath("//text()").getall()
            truck_size = "".join(truck_size_content)
        except:truck_size="N/A"

        try:
            truck_capacity_check = _.get(truck_details,'passing_ton_range_text',"N/A")
            truck_capacity_selector = Selector(text=truck_capacity_check)
            truck_capacity_content = truck_capacity_selector.xpath("//text()").getall()
            truck_capacity = "".join(truck_capacity_content)
        except:truck_capacity = "N/A"

        item = {
            "input_start_location" : source,
            "input_end_location" : destination,

            "distance" : distance,
            "price_per_truck" : price_per_truck,
            "to_location" : to_location,
            "from_location" : from_location,
            "product_type" : product_type,

            "truck_type" : truck_type,
            "truck_size" : truck_size,
            "truck_capacity" : truck_capacity,

            "customer_id" : customer_id,
            "customer_business_name" : customer_business_name,
            "customer_profile_type" : customer_profile_type,
            "customer_verification_status" : customer_verification_status,
            "customer_proprietor_name" : customer_proprietor_name,
            "customer_review_count" : customer_review_count,
            "customer_rating" : customer_rating,
            "customer_location" : customer_location,
            "customer_contact" : customer_contact
        }
        final_result.append(item)


if __name__ == '__main__':
    MAX_THREADS = 1
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(process_row, row) for _, row in df.iterrows()]


if final_result:
    print(final_result)
    OUTPUT_FILE = "blackbuck_sample.xlsx"
    df = pd.DataFrame(final_result)
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"📁 Excel saved: {OUTPUT_FILE} with {len(final_result)} items.")
else:
    print("❌ No menu items found to save.")