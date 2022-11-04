FROM rabbitmq-python-base:0.0.1

COPY likes_filter/likes_filter.py /root/likes_filter.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/likes_filter.py"]
