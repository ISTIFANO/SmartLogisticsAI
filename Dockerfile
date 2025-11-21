FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y default-jre-headless wget curl git && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

COPY app/ /app/

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

RUN pip install jupyter pandas matplotlib seaborn pymongo

EXPOSE 8501 8888

CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
