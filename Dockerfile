FROM python:3.8.5-slim-buster

RUN apt-get update -y \
    && apt-get install -y gcc libmariadb-dev\
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /inpe_stac
WORKDIR /inpe_stac

COPY inpe_stac/ /inpe_stac
COPY requirements.txt /inpe_stac
RUN pip install -r requirements.txt

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]
