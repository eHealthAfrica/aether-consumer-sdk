FROM python:3.8-slim-buster

WORKDIR /code
ENTRYPOINT ["/code/entrypoint.sh"]

COPY ./requirements.txt /code/requirements.txt

ENV VENV_DIR=/var/run/aether/venv
ENV PATH="$VENV_DIR/bin:$PATH"

RUN apt-get update -qq > /dev/null && \
    apt-get -qq \
        --yes \
        --allow-downgrades \
        --allow-remove-essential \
        --allow-change-held-packages \
        install gcc > /dev/null && \
    mkdir -p $VENV_DIR && \
    python3 -m venv $VENV_DIR && \
    pip install -q --upgrade pip && \
    pip install -q -r requirements.txt

COPY ./ /code/
