FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.28.0 \
    mysql-connector-python==8.2.0 \
    pandas==2.0.3 \
    plotly==5.17.0

COPY dashboard.py .

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.address=0.0.0.0"]