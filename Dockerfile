FROM python:3.7-slim-stretch

WORKDIR /code

RUN apt-get update -qq && \
    apt-get -qq \
        --yes \
        --allow-downgrades \
        --allow-remove-essential \
        --allow-change-held-packages \
        install gcc

COPY ./requirements.txt /code/requirements.txt

RUN pip install -q --upgrade pip && \
    pip install -q -r requirements.txt

COPY ./ /code/

ENTRYPOINT ["/code/entrypoint.sh"]
