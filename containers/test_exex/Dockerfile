FROM python:3.9.7-slim
RUN pip3 install pika

COPY test_exex/test_exex.py /root/test_exex.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/test_exex.py"]
