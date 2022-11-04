FROM rabbitmq-python-base:0.0.1

COPY duplicates_filter/duplicates_filter.py /root/duplicates_filter.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/duplicates_filter.py"]
