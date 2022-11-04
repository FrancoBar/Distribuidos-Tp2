FROM rabbitmq-python-base:0.0.1

COPY request_listener/request_listener.py /root/request_listener.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/request_listener.py"]