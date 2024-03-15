FROM python:3.11-slim

RUN mkdir queue_guard

WORKDIR /queue_guard

COPY . /queue_guard

RUN apt-get update -y

RUN pip install -r requirements.txt

CMD ["python","server_rpc.py"]
