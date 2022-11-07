FROM python
RUN pip3 install pika

COPY likes_filter/likes_filter.py /root/likes_filter.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/likes_filter.py"]
