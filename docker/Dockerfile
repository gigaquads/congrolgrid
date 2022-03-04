FROM python:3.9

COPY . /usr/src/app
VOLUME /usr/src/app
WORKDIR /usr/src/app

# prevents Python from buffering stdout and stderr
ENV PYTHONUNBUFFERED 1

RUN python setup.py develop

EXPOSE 8000

CMD uvicorn controlgrid.api.app:app --reload --host 0.0.0.0
