FROM python:3.7-alpine
RUN apk update
RUN apk add docker
COPY common /root/common
COPY test_dind/test_dind.py /root/test_dind.py
ENTRYPOINT ["python3", "/root/test_dind.py"]