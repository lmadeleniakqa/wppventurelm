FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime
WORKDIR /app
RUN pip install --no-cache-dir google-cloud-bigquery google-cloud-storage pandas scikit-learn xgboost db-dtypes
COPY model.py .
COPY train.py .
ENTRYPOINT ["python", "train.py"]
