FROM python
RUN pip3 install pika

COPY adaptor1-2/adaptor1-2.py /root/adaptor1-2.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/adaptor1-2.py"]
