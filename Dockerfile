FROM python:3.7-slim-stretch

WORKDIR /code
COPY ./requirements.txt /code/requirements.txt

RUN apt-get update -qq && \
    apt-get -qq \
        --yes \
        --allow-downgrades \
        --allow-remove-essential \
        --allow-change-held-packages \
        install gcc && \
    pip install -q --upgrade pip && \
    pip install -q -r requirements.txt

COPY ./ /code/

ENTRYPOINT ["/code/entrypoint.sh"]
