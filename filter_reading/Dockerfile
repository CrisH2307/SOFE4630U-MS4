FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
COPY service_account.json .
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/service_account.json"
CMD ["python", "-u", "./main.py"]