FROM python:3.9.7-slim
RUN pip3 install pika

COPY test_extcp/test_extcp.py /root/test_extcp.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/test_extcp.py"]
