ARG BASE
FROM ${BASE}
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends libpq5 libyaml-dev build-essential pkg-config && rm -rf /var/lib/apt/lists/*
RUN gem install rails -v 8.1.4.1 --no-document && gem install pg -v 1.7.0 --no-document
COPY . /app
ENTRYPOINT ["ruby", "/app/probe.rb"]
