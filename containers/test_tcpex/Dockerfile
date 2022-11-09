FROM python:3.9.7-slim
RUN pip3 install pika

COPY test_tcpex/test_tcpex.py /root/test_tcpex.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/test_tcpex.py"]
