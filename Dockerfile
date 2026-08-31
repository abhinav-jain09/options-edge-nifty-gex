FROM python:3.12-slim
WORKDIR /app
COPY gex_service.py .
ENV DHAN_GEX_BIND=0.0.0.0 \
    PYTHONUNBUFFERED=1
EXPOSE 8188
USER 65534
CMD ["python3", "gex_service.py"]
