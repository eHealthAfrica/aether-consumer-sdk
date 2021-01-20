FROM python:3.8-slim-buster

WORKDIR /code
ENTRYPOINT ["/code/entrypoint.sh"]

COPY ./requirements.txt /code/requirements.txt

RUN apt-get update -qq > /dev/null && \
    apt-get -qq \
        --yes \
        --allow-downgrades \
        --allow-remove-essential \
        --allow-change-held-packages \
        install gcc > /dev/null && \
    pip install -q --upgrade pip && \
    pip install -q -r requirements.txt

COPY ./ /code/
