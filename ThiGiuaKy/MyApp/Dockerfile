# build app từ image python:3.9.11 (image python:3.9.11 sẽ có khi load thành công app_khdl16a1.tar)
FROM python:3.9.11

WORKDIR /app

COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /requirements.txt && pip cache purge
COPY . .

CMD ["python", "app.py"]