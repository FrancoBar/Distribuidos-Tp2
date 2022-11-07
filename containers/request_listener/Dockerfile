FROM python
RUN pip3 install pika

COPY request_listener/request_listener.py /root/request_listener.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/request_listener.py"]