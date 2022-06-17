FROM ghcr.io/deephaven/server:edge
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
COPY data/app.d /app.d
