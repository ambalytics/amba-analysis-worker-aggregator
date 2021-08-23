FROM python:3.6

WORKDIR /src
COPY . .

RUN pip install --upgrade pip

COPY src/requirements.txt /requirements.txt
RUN pip install -r src/requirements.txt
COPY ./src/aggregator.py /aggregator.py
#RUN cd src
#CMD ls
#CMD ["faust", "-A", "aggregator", "worker", "-l", "info"]
CMD [ "python", "./src/aggregator.py", "worker", "-l", "info"]