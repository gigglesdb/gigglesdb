FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1
WORKDIR /probe
RUN pip install --no-cache-dir 'psycopg[binary]==3.2.9'
COPY probe.py /probe/probe.py
ENTRYPOINT ["python", "-B", "/probe/probe.py"]
