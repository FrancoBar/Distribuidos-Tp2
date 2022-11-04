FROM rabbitmq-python-base:0.0.1

COPY tag_filter/tag_filter.py /root/tag_filter.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/tag_filter.py"]
