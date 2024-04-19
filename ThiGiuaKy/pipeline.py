import uuid
import pymongo
import datetime as dt
import json
import random

# from midtermDAG import DAG
# from midtermOperator import BashOperator
# from midtermOperator import PythonOperator

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct

# Hoàn thành việc kết nối với MongoDB và Qdrant
qdrant_client = QdrantClient(host="qdrant_db", port=6333)
mongo_client = pymongo.MongoClient("mongodb://admin:admin@mongodb:27017")
database_mongo = mongo_client["midterm"]
# tạo collection có tên là mssv của bạn trong MongoDB và Qdrant
# ví dụ mssv của bạn là 17101691 thi tên collection sẽ là "17101691"
collection_mongo = "21130221"
name_collection_qdrant = "21130221"


def create_collection_qdrant():
    try:
        # lấy tên tất cả collection hiện có trong Qdrant
        name_collection_qdrant = "21130221"
        qdrant_client = QdrantClient(host="qdrant_db", port=6333)
        collections = qdrant_client.get_collections()
        collectionNames = [
            collection.name for collection in collections.collections
        ]

        # cấu hình vector params cho collection bao gồm size = 1536 và distance = cosine
        vectorParams = {
            "size": 1536,
            "distance": Distance.COSINE
        }

        # kiểm tra nếu collection chưa tồn tại thì tạo mới
        if name_collection_qdrant not in collectionNames:
            qdrant_client.recreate_collection(
                collection_name=name_collection_qdrant,
                vectors_config=VectorParams(**vectorParams)
            )
            print("Create collection successfully")
        else:
            print("Collection already exists")

        return {
            "status": "success",
            "collection": name_collection_qdrant,
            "vectorParams": str(vectorParams)
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


def insert_data_mongoDB():
    try:
        # đọc dữ liệu từ file data_iuh_new.json và chọn ngẫu nhiên điểm dữ liệu gán vào biến data
        mongo_client = pymongo.MongoClient(
            "mongodb://admin:admin@mongodb:27017")
        with open('/opt/airflow/dags/data_iuh_new.json', "r") as f:
            data = json.load(f)
        random_data = random.choice(data)
        database_mongo = mongo_client["midterm"]
        news = database_mongo["news"]
        message = ""

        # Kiểm tra title của điểm dữ liệu đã tồn tại trong MongoDB chưa
        title = random_data["title"]

        status = "new"
        if news.find_one({"title": title}):
            message = "Data already exists"
        else:
            random_data["status"] = "new"
            result = news.insert_one(random_data)
            message = "Data inserted"

        return {
            "status": "success",
            "data": title,
            "message": message
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


def insert_data_qdrant():
    try:
        # Connect to MongoDB
        mongo_client = pymongo.MongoClient(
            "mongodb://admin:admin@mongodb:27017")
        database_mongo = mongo_client["midterm"]
        news = database_mongo["news"]

        # Connect to Qdrant
        qdrant_client = QdrantClient(host="qdrant_db", port=6333)

        # Read data from MongoDB
        for new in news.find({"status": "new"}):
            mongo_id = new['_id']  # Keep the original MongoDB _id
            id = str(new.pop('_id'))

            # Get the 'embedding' field
            vector = new.pop('embedding', None)
            if vector is None or len(vector) != 1536:
                continue  # Skip this document if it doesn't have a valid vector

            # Create a point for Qdrant
            point = PointStruct(id=str(uuid.uuid4()),
                                vector=vector, payload=new)
            qdrant_client.upsert(
                collection_name=name_collection_qdrant, points=[point])

            # Update status in MongoDB
            news.update_one({'_id': mongo_id}, {'$set': {'status': 'indexed'}})
            print(f"Document with id {mongo_id} has been indexed")

        return {
            "status": "success",
            "message": "Data inserted into Qdrant"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


def count_data():
    try:
        collection_name = "21130221"
        name_collection_qdrant = "21130221"
        qdrant_client = QdrantClient(host="qdrant_db", port=6333)
        mongo_client = pymongo.MongoClient(
            "mongodb://admin:admin@mongodb:27017")
        collection_mongo = mongo_client["midterm"][collection_name]
        count = collection_mongo.count_documents({})
        count_indexed = collection_mongo.count_documents({"status": "indexed"})
        count_new = collection_mongo.count_documents({"status": "new"})
        return {
            "status": "success",
            "indexed": count_indexed,
            "new": count_new,
            "mongoDB": count,
            "vectorDB": qdrant_client.get_collection(collection_name=name_collection_qdrant).points_count
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


def search_by_vector():
    try:
        name_collection_qdrant = "21130221"
        qdrant_client = QdrantClient(host="qdrant_db", port=6333)
        # Tạo ngẫu nhiên một vector có size = 1536
        random_vector = [random.random() for _ in range(1536)]

        # Sử dụng Qdrant để tìm kiếm 1 điểm gần nhất
        result = qdrant_client.search(
            collection_name=name_collection_qdrant, query_vector=random_vector, limit=1)

        # Lấy kết quả dưới dạng JSON nếu có
        result_item = next(iter(result), None)
        if result_item:
            result_json = result_item.model_dump()
            return {
                "status": "success",
                "result": result_json
            }
        else:
            return {
                "status": "success",
                "result": "No results found"
            }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


# Set owner as your student ID, retry once if failed, wait time between retries is 1 minute
default_args = {
    'owner': '21130221',
    'start_date': dt.datetime.now() - dt.timedelta(minutes=19),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

# Initialize DAG with your student ID as the name, run every 5 minutes
with DAG('21130221',
         default_args=default_args,
         tags=['midterm'],
         schedule_interval=dt.timedelta(minutes=5),
         ) as dag:

    # Task 1: Use BashOperator to print "Midterm exam started" with task_id as your student ID
    task1 = BashOperator(task_id='21130221',
                         bash_command='echo "Midterm exam started"')

    # Task 2: Use PythonOperator to create collection in Qdrant with task_id as the first 4 digits of your student ID
    task2 = PythonOperator(
        task_id='2113', python_callable=create_collection_qdrant)

    # Task 3: Use PythonOperator to insert data into MongoDB with task_id as the last 3 digits of task2's task_id and the next digit in your student ID
    task3 = PythonOperator(
        task_id='1130', python_callable=insert_data_mongoDB)

    # Task 4: Use PythonOperator to insert data into Qdrant with task_id as the last 3 digits of task3's task_id and the next digit in your student ID
    task4 = PythonOperator(
        task_id='1302', python_callable=insert_data_qdrant)

    # Task 5: Use PythonOperator to count data with task_id as the last 3 digits of task4's task_id and the next digit in your student ID
    task5 = PythonOperator(
        task_id='3022', python_callable=count_data)

    # Task 6: Use PythonOperator to search by vector with task_id as the last 3 digits of task5's task_id and the next digit in your student ID
    task6 = PythonOperator(
        task_id='0221', python_callable=search_by_vector)

    # Task 7: Use BashOperator to print "Midterm exam ended" with task_id as the first 2 digits and the last 2 digits of your student ID
    task7 = BashOperator(
        task_id='2121', bash_command='echo "Midterm exam ended"')

    # Define task order
    task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7
