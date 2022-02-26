FROM python:3.9

COPY . /usr/src/app
VOLUME /usr/src/app
WORKDIR /usr/src/app

RUN python setup.py develop

CMD uvicorn controlgrid.api.app:app --reload
