#!/usr/bin/env python
# coding: utf-8
# 學號：
# 姓名：
# 誠信聲明：本人同意並遵守期末實作考試之學術誠信規範，保證本程式碼完全由本人獨立實作完成，絕無抄襲或代寫等舞弊行為。
# 簽名：____________________
"""
東吳大學 114 學年度第 2 學期期末考試題 — 資料工程 (114_DE4VD_finalExam.py)
"""

import os
import requests
import hashlib
import psycopg2
import logging
import datetime as dt
from datetime import timedelta
from xml.dom.minidom import parseString
from elasticsearch import Elasticsearch, helpers
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

# 台北時區
local_tz = pendulum.timezone("Asia/Taipei")

# ==== 0. 日誌設定 ====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# ==== 1. 資料來源 URL ====
VD_INFO_URL = "https://tisvcloud.freeway.gov.tw/history/motc20/VD.xml"
VD_DATA_URL = "https://tisvcloud.freeway.gov.tw/history/motc20/VDLive.xml"

# 原始資料備份路徑
RAW_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'raw')
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# ==== 2. 連線與個人學號參數設定 ====
STUDENT_ID = "" #請輸入學號
ES_HOST = "http://localhost:9200"

DB_HOST = "localhost"
DB_PORT = 5432
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_NAME = f"de_exam_{STUDENT_ID}"


try:
    STUDENT_ID_LAST_DIGIT = int(STUDENT_ID[-1]) if STUDENT_ID[-1].isdigit() else None
    if STUDENT_ID_LAST_DIGIT == 0:
        STUDENT_ID_LAST_DIGIT = 1
except Exception:
    STUDENT_ID_LAST_DIGIT = None


def get_text(item):
    """取得 XML 節點底下的文字資料"""
    if len(item.childNodes) == 0:
        return ""
    return item.childNodes[0].data


def download_and_parse_xml(url, filename):
    for attempt in range(1, 4):
        try:

            # ------ CODE BEGIN ZONE 0 ------
            # 【題目三 - 1：ZONE 0】
            r = requests.get(url, )
            






            # ------ CODE END ZONE 0 ------
            r.raise_for_status()
            r.encoding = 'utf-8'
            
            # 先行解析以確保 XML 資料完整無缺損 (防範下載不完全之情況)
            dom = parseString(r.text)
            
            # 確保解析無誤後再進行備份
            filepath = os.path.join(RAW_DATA_DIR, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(r.text)
                
            logging.info(f"下載與解析成功，並已備份至 {filepath}")
            return dom
        except Exception as e:
            logging.warning(f"下載失敗 (嘗試 {attempt}/3): {e}")
            if attempt == 3:
                raise e


def parse_vd_info(xml):
    info = {} 

    for VD in xml.getElementsByTagName("VD"):
        vd_info = {}
        vd_info.update({"VDID": get_text(VD.getElementsByTagName("VDID")[0])})
        # ------ CODE BEGIN ZONE 1 ------
        # 【題目三 - 2：ZONE 1】
        
        vd_info.update({"SubAuthority": get_text(VD.getElementsByTagName("SubAuthority")[0]) if VD.getElementsByTagName("SubAuthority") else ""})
        vd_info.update({"BiDirectional": })
        vd_info.update({"VDType": })
        vd_info.update({"LocationType": })
        vd_info.update({"DetectionType": })
        vd_info.update({"PositionLon": })
        vd_info.update({"PositionLat": })
        vd_info.update({"RoadID": })
        vd_info.update({"RoadName": })
        vd_info.update({"RoadClass": })








        # ------ CODE END ZONE 1 ------
        
        road_section = VD.getElementsByTagName("RoadSection")[0] if VD.getElementsByTagName("RoadSection") else None
        start_node = road_section.getElementsByTagName("Start")[0] if road_section else None
        end_node = road_section.getElementsByTagName("End")[0] if road_section else None
        vd_info.update({"RoadSectionStart": get_text(start_node) if start_node else ""})
        vd_info.update({"RoadSectionEnd": get_text(end_node) if end_node else ""})
        
        info.update({vd_info["VDID"]: vd_info})
        
    return info


def parse_vd_data(xml):
    UpdateTime = get_text(xml.getElementsByTagName("UpdateTime")[0])
    data = {}

    for VD in xml.getElementsByTagName("VDLive"):
        vd_data = {"UpdateTime": UpdateTime}
        # ------ CODE BEGIN ZONE 2-1 ------
        # 【題目三 - 3：ZONE 2-1】
        vd_data.update({"VDID": get_text(VD.getElementsByTagName("VDID")[0])})
        vd_data.update({"LinkID": })
        vd_data.update({"LaneID": })
        vd_data.update({"LaneType": })
        vd_data.update({"Speed": })
        vd_data.update({"Occupancy": })









        # ------ CODE END ZONE 2-1 ------
        
        total_vol = 0
        for vehicle in VD.getElementsByTagName("Vehicle"):
            vol_str = get_text(vehicle.getElementsByTagName("Volume")[0]) if vehicle.getElementsByTagName("Volume") else "0"
            total_vol += int(vol_str) if vol_str.isdigit() else 0
        vd_data.update({"Volume": total_vol})
        
        # ------ CODE BEGIN ZONE 2-2 ------
        # 【題目三 - 3：ZONE 2-2】
        try:
            sp = float(vd_data["Speed"])
            occ = float(vd_data["Occupancy"])
            vol = float(vd_data["Volume"])
        except ValueError:
            
            
        if :
            logging.warning()
            continue
        if :
            logging.warning()
            continue




        # ------ CODE END ZONE 2-2 ------
            
        data.update({vd_data["VDID"]: vd_data})
        
    return data


def transform(info, data):
    processed_data = []

    for key in data:
        if key not in info:
            continue
        d = {}
        d.update(info[key])
        d.update(data[key])
        d["id"] = data[key]["UpdateTime"] + "|" + data[key]["VDID"]
        volume_total = data[key]["Volume"]
        volume_adjusted = volume_total * (1 + STUDENT_ID_LAST_DIGIT * 0.05)
        d["Volume_adjusted"] = round(volume_adjusted, 2)

        # ------ CODE BEGIN ZONE 3 ------
        # 【題目三 - 4：ZONE 3】
        d["coords"] = 
        d["creator_by"] = 




        # ------ CODE END ZONE 3 ------
        
        processed_data.append(d)

    return processed_data


def insertData(data):
    # 1. 寫入 Elasticsearch
    es = Elasticsearch(ES_HOST)
    es_index = f"vd_raw_index_{STUDENT_ID}"
    
    mappings = {
        "mappings": {
            "properties": {
                "vd_id": {"type": "keyword"},
                "road_name": {"type": "text"},
                "speed": {"type": "integer"},
                "occupancy": {"type": "integer"},
                "volume": {"type": "integer"},
                "volume_adjusted": {"type": "float"},
                "location": {"type": "geo_point"},
                "updated_at": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy/MM/dd HH:mm:ss||strict_date_optional_time||epoch_millis"}
            }
        }
    }
    if not es.indices.exists(index=es_index):
        es.indices.create(index=es_index, body=mappings)
        
    es_actions = []
    for d in data:
        try:
            lat = float(d["PositionLat"])
            lon = float(d["PositionLon"])
        except ValueError:
            continue
            
        # ------ CODE BEGIN ZONE 4-1 ------
        #【題目三 - 5：ZONE 4-1】
        es_actions.append({
            "_index": es_index,
            "_id": d["VDID"] + "_" + STUDENT_ID,
            "_source": {
                "vd_id": ,
                "road_name": ,
                "speed": ,
                "occupancy": ,
                "volume": ,
                "volume_adjusted": ,
                "location": ,
                "updated_at": 
            }
        })





        # ------ CODE END ZONE 4-1 ------
    if es_actions:
        helpers.bulk(es, es_actions)
        logging.info(f"成功批次寫入 {len(es_actions)} 筆原始資料至 ES。")

    # 2. 寫入 PostgreSQL
    try:
        conn_default = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database="postgres")
        conn_default.autocommit = True
        cur_default = conn_default.cursor()
        cur_default.execute(f"SELECT 1 FROM pg_database WHERE datname = 'de_exam_{STUDENT_ID}'")
        if not cur_default.fetchone():
            cur_default.execute(f"CREATE DATABASE de_exam_{STUDENT_ID}")
        cur_default.close()
        conn_default.close()
    except Exception:
        pass

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
    cur = conn.cursor()
    table_name = f"vd_metrics_{STUDENT_ID}"
    
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        vd_id VARCHAR(50) PRIMARY KEY,
        road_name VARCHAR(100),
        speed NUMERIC(5, 2),
        occupancy NUMERIC(5, 2),
        volume_adjusted NUMERIC(10, 2),
        coords VARCHAR(50),
        row_hash VARCHAR(50),
        creator_by VARCHAR(100),
        updated_at TIMESTAMP
    );
    """)

    pg_actions = []
    for d in data:
        vol_str = f"{d['Volume_adjusted']:.2f}"
        hash_str = f"{STUDENT_ID}{d['VDID']}{vol_str}"
        row_hash = hashlib.md5(hash_str.encode('utf-8')).hexdigest()

        try:
            dt_clean = d["UpdateTime"].replace("T", " ").split("+")[0]
            updated_at = dt.datetime.strptime(dt_clean, "%Y-%m-%d %H:%M:%S")
        except Exception:
            updated_at = dt.datetime.now()

        pg_actions.append((
            d["VDID"], d["RoadName"], float(d["Speed"]), float(d["Occupancy"]),
            float(d["Volume_adjusted"]), d["coords"], row_hash, d["creator_by"], updated_at
        ))

    # ------ CODE BEGIN ZONE 4-2 ------
    # 【題目三 - 5：ZONE 4-2】
    upsert_query = f"""
    
    """
    if pg_actions:
        cur.executemany(upsert_query, pg_actions)
        conn.commit()




    # ------ CODE END ZONE 4-2 ------
    
    cur.close()
    conn.close()


def main():
    if STUDENT_ID == "請輸入學號" or STUDENT_ID_LAST_DIGIT is None:
        logging.error("請先設定學號參數！")
        return

    # 下載 XML
    xml_vd_info = download_and_parse_xml(VD_INFO_URL, "VD.xml")
    xml_vd_data = download_and_parse_xml(VD_DATA_URL, "VDLive.xml")

    # 解析 XML
    vd_info = parse_vd_info(xml_vd_info)
    vd_data = parse_vd_data(xml_vd_data)

    # 轉換與關聯
    data = transform(vd_info, vd_data)

    # 寫入儲存庫
    insertData(data)
    print("Finished...")  


# ==== 3. Set Airflow DAG's config ====
default_args = {
    # ------ CODE BEGIN ZONE 5-1 ------
    # 【題目四 - 1：ZONE 5-1】
    'owner': ,
    'start_date': dt.datetime(2026, 6, 11, tzinfo=local_tz),
    'retries': ,
    'retry_delay': dt.timedelta(),




    # ------ CODE END ZONE 5-1 ------
}


# ------ CODE BEGIN ZONE 5-2 ------
# 【題目四 - 2：ZONE 5-2】
with DAG(
    '',
    default_args=default_args,
    schedule=timedelta(),
    catchup=,
) as dag:




# ------ CODE END ZONE 5-2 ------

    doProcessData = PythonOperator(
        task_id='getVDtoDataWarehouse',
        python_callable=main
    )
