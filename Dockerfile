FROM python:3.10-sli

# Install Java for PySpark and utilities
RUN apt-get update && \
    apt-get install -y default-jre-headless wget curl git && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set working directory
WORKDIR /app

# Copy the app code (Streamlit & notebooks)
COPY app/ /app/

# Upgrade pip and install Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Install Jupyter and common data science packages
RUN pip install jupyter pandas matplotlib seaborn pymongo

# Expose Streamlit and Jupyter ports
EXPOSE 8501 8888

CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
