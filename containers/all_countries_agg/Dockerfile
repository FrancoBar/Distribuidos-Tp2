FROM python
RUN pip3 install pika

COPY all_countries_agg/all_countries_agg.py /root/all_countries_agg.py
COPY common /root/common
ENTRYPOINT ["python3", "/root/all_countries_agg.py"]
