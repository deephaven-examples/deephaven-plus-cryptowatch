FROM ghcr.io/deephaven/server:latest
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
COPY data/app.d /app.d
