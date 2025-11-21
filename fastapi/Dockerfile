FROM python:3.10-slim

# Install Java (required for PySpark) and common utilities
RUN apt-get update && \
	apt-get install -y openjdk-21-jre wget curl git && \
	rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

COPY . /app

# Use a requirements file to control Python deps (including pyspark)
RUN pip install --upgrade pip
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; else pip install --no-cache-dir fastapi uvicorn websockets pyspark; fi

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
