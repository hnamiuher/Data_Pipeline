
import random
import flask
from flask import request, make_response
from qdrant_client import QdrantClient

app = flask.Flask(__name__)


def search(query):
    # đặt tên collection bạn đã tạo khi thực hiện pipeline vào biến bên dưới
    collection_name = "21130221"
    # thực hiện kết nối với cơ sở dữ liệu qdrant
    client = QdrantClient(host="qdrant_db", port=6333)
    collections = client.get_collections()
    collectionNames = [
        collection.name for collection in collections.collections]
    embedding = [random.random() for i in range(1536)]
    if collection_name in collectionNames:
        # thực hiện tìm kiếm với vector embedding bên trên
        results = client.search(
            collection_name=collection_name,
            query_vector=embedding,
            limit=1,
        )

        result_json = results[0].model_dump()
        return {
            "status": "success",
            "query": query,
            "result": result_json
        }
    return {"message": "Collection not found"}


@app.route('/health', methods=['GET'])
def healthCheck():
    # điền mssv của bạn vào bên dưới
    return {
        "status": "success",
        "student_id": "21130221",
    }

# Viết api có route "/search", phương thức POST để nhận vào câu query từ phía ngưới dùng
# sử dụng hàm search bên trên để tìm kiếm và trả về kết quả của hàm search cho người dùng


@app.route('/search', methods=['POST'])
def searchView():
    query = request.json["query"]
    result = search(query)
    return make_response(result)


if __name__ == '__main__':
    # run app với port là "99xy" với xy là 2 số cuối cùng của mssv của bạn. Ví dụ: mssv: 17101691 thì port=9991
    app.run(host='0.0.0.0', port=9951, debug=True)
    # curl -X POST http://localhost:8989/search -H "Content-Type: application/json" -d '{"query": "hội nghị cấp khoa"}'
