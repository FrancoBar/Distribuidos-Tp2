FROM python
RUN pip3 install pika

COPY max_day_filter/max_day_filter.py /root/max_day_filter.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/max_day_filter.py"]
