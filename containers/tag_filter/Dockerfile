FROM python
RUN pip3 install pika

COPY tag_filter/tag_filter.py /root/tag_filter.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/tag_filter.py"]
