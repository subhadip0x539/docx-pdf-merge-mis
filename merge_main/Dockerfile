FROM python:3.11-slim

RUN mkdir merge_main

WORKDIR /merge_main

COPY . /merge_main

RUN apt-get update -y

RUN apt-get install build-essential -y

RUN pip install -r requirements.txt

EXPOSE 50051

CMD ["python","server.py"]
