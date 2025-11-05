import threading
import time
import pandas as pd
import requests
# from curl_cffi import requests
import json

File_name = "blackbuck_app"
thread_counter = 2
loop_counter = 1000000

datas = []

proxy = {
   "http": 'http://f42a5b59aec3467e97a8794c611c436b91589634343:super=false@proxy.scrape.do:8080',
   "https": 'http://f42a5b59aec3467e97a8794c611c436b91589634343:super=false@proxy.scrape.do:8080',
}

def fetch_url():
   try:

       headers = {
           "Accept-Encoding": "gzip",
           "Accept-Language": "en",
           "Android-Version": "28",
           "App-Version": "1100",
           "Authorization": "Token eyJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3NjIzMjEwNTUsInJhbmRvbWl6ZXIiOiJjZmNhODFjMS0xZGU2LTRhYmYtOTdjNy02MTA2ZDIwOThhZGIiLCJ1c2VyQ3JlZGVudGlhbHNJZCI6IjY5MDk5OTcxNWNkMjIwMWRmMmJlMTkwYyIsInRlbmFudCI6IlNVUFBMWSIsImNsaWVudElkZW50aWZpZXIiOiI3MjQzNzI4IiwiY2xpZW50IjoiQk9TU19BUFAiLCJvbGRDbGllbnRJZCI6MjcsInR0bCI6NTE4NDAwMDAwMCwiaXNHdWVzdFVzZXIiOmZhbHNlLCJzZXNzaW9uVHlwZSI6Ik5PTl9NSU1JQyIsImlzcyI6IkFVVEhFTlRJQ0FUSU9OIn0.qxObPv-VDZ66Wzi19j-3UuWm7rjGtj-5Ym1guxszQW8",
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

       payload = {
           "call_logs_permission_enabled": False,
           "draw_over_apps_permission_enabled": False,
           "from_google_place_id": "ChIJLbZ-NFv9DDkRQJY4FbcFcgM",
           "from_location_coordinates": None,
           "from_location_name": "Delhi",
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
           "date": 1762321079274,
           "to_google_place_id": "ChIJwe1EZjDG5zsRaYxkjY_tpF0",
           "to_location_coordinates": None,
           "to_location_name": "Mumbai",
           "to_zone_id": None,
           "truck_attribute_list": None,
           "truck_number": None
       }

       url = "https://loadboard-api-gateway.blackbuck.com/supplywrapper/bookload/v5/getIndentList"

       for i in range(loop_counter):  # Number of iterations
           list_1 = []
           start_tm = time.time()
           response = requests.post(url,
                                   headers=headers,
                                   data=json.dumps(payload),
                                   # impersonate="chrome120",
                                    timeout=200,
                                   proxies=proxy, #Use the proxy if available and if need t use.
                                   verify=False,
                                   )
           # print(response.text)
           # print(response.headers)
           status_response = "good response..." if "₹ 58,000 per ton" in response.text else "bad response..."
           print(status_response)
           end_tm = time.time() - start_tm
           list_1.extend([url, response.status_code, status_response, end_tm])
           datas.append(list_1)
   except Exception as e:
       print(f"Request failed: {e}")
       datas.append([url, 'Failed', 'bad response...', 0])

threads = []

# Create threads
for _ in range(thread_counter):
   thread = threading.Thread(target=fetch_url)
   threads.append(thread)
   thread.start()

# Wait for all threads to finish
for thread in threads:
   thread.join()

# Save to Excel
df = pd.DataFrame(datas, columns=['URL', 'Status Code', 'Response Status', 'Time Taken'])
df.to_excel(f'{File_name}.xlsx', index=False)
print(f'All requests completed. Excel file saved as {File_name}.xlsx')
