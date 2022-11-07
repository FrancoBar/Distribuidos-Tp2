# FROM rabbitmq-python-base:0.0.1
FROM python
RUN pip3 install pika

COPY test_exchange_output/output.py /root/output.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/output.py"]
