FROM python:3.6

WORKDIR /src
COPY . .

RUN pip install --upgrade pip

COPY src/requirements.txt /requirements.txt
RUN pip install -r src/requirements.txt
COPY ./src/aggregator.py /aggregator.py

#CMD ["faust", "-A", "aggregator", "worker", "-l", "info"]
#CMD [ "python", "./src/aggregator.py", "worker", "-l", "info"]
#ENTRYPOINT ["/bin/bash", "-c", "./scripts/entrypoint.sh"]
ENTRYPOINT ["sh", "./scripts/entrypoint.sh"]