FROM rabbitmq-python-base:0.0.1

COPY max_day_agg/max_day_agg.py /root/max_day_agg.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/max_day_agg.py"]
