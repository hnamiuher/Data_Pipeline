# Hoàn thành dockerfile để build image cho airflow từ image airflow_khdl16a1
# với các yêu cầu trong file requirements.txt
FROM apache/airflow:2.8.2
RUN pip install apache-airflow==${AIRFLOW_VERSION}
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
