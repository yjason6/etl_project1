FROM python:3.9.16-slim

WORKDIR /src

ENV api_key="TyfX4zH87A9CmeZI7CUWHpCayinLIXKF"
ENV db_user="postgres"
ENV db_password="neXia2544!?"
ENV db_server_name="database-1.c7ah9wgi3bni.ap-southeast-2.rds.amazonaws.com"
ENV db_database_name="postgres"
ENV PYTHONPATH=/src

COPY /project1 .

RUN pip install -r requirements.txt

CMD ["python", "pipeline/project1_pipeline.py"]
