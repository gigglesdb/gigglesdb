ARG BASE
FROM ${BASE}
ENV PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
RUN pip install --no-cache-dir Django==5.2.17 SQLAlchemy==2.0.57 alembic==1.20.1 psycopg[binary]==3.3.8
COPY . /app
ENTRYPOINT ["python", "-B", "/app/probe.py"]
