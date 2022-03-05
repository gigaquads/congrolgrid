FROM python:3.9

COPY . /usr/src/app
VOLUME /usr/src/app
WORKDIR /usr/src/app

# prevents Python from buffering stdout and stderr
ENV PYTHONUNBUFFERED 1

# number of worker processes for uvicorn. N/A when --reload is used.
ENV WEB_CONCURRENCY 4

RUN python setup.py develop

EXPOSE 8000

CMD uvicorn controlgrid.api.app:app --host 0.0.0.0
