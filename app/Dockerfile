FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir yfinance pandas
COPY server.py updater.py ./
COPY static/ static/
EXPOSE 8080
CMD ["python", "server.py"]
