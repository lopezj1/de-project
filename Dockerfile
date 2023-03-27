FROM python:3.9-slim-buster

RUN apt-get update && apt-get install -y gcc libpq-dev && apt-get clean

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "app.py" ]