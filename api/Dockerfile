FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    flask==2.3.3 \
    flask-cors==4.0.0 \
    mysql-connector-python==8.2.0

COPY app.py .

EXPOSE 5000

CMD ["python", "app.py"]