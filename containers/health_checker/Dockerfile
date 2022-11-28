FROM python:3.7-alpine
RUN apk update
RUN apk add docker
COPY common /root/common
COPY health_checker/election /root/election
COPY health_checker/health_checker.py /root/health_checker.py
ENTRYPOINT ["python3", "/root/health_checker.py"]