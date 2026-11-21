FROM python:3.12-slim-bookworm AS compiler
RUN apt-get update && apt-get install -y --no-install-recommends g++
COPY native/ /build/
RUN g++ -O3 -DNDEBUG -std=c++17 -pthread -Wall -Wextra /build/ingest.cpp -o /ingest

FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY --from=compiler /ingest /app/ingest
COPY pipeline/ /app/pipeline/
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
CMD ["/app/run.sh"]
