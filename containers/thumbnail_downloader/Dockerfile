FROM python
RUN pip3 install pika

COPY thumbnail_downloader/thumbnail_downloader.py /root/thumbnail_downloader.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/thumbnail_downloader.py"]
