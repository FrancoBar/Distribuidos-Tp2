FROM python:3.9.7-slim
RUN pip3 install pika

COPY test_exchange_output/test_exchange_output.py /root/test_exchange_output.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/test_exchange_output.py"]
