FROM rabbitmq-python-base:0.0.1

COPY all_countries_agg/all_countries_agg.py /root/all_countries_agg.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/all_countries_agg.py"]
