FROM rabbitmq-python-base:0.0.1

COPY thumbnail_downloader/thumbnail_downloader.py /root/thumbnail_downloader.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/thumbnail_downloader.py"]
