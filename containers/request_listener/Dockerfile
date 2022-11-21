FROM python
RUN pip3 install pika
RUN pip3 install psutil

COPY request_listener/request_listener.py /root/request_listener.py
COPY request_listener/client_handler.py /root/client_handler.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/request_listener.py"]