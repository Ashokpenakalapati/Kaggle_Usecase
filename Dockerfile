FROM python:3.11-slim-bookworm
ENV PYTHONUNBUFFERED True
WORKDIR /app
COPY . .
COPY requirements.txt ./
RUN apt-get update
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update
ENTRYPOINT [ "python3", "main.py" ]
