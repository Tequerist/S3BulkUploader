FROM python:3.10

COPY . /usr/src/

WORKDIR /usr/src/

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

ENTRYPOINT ["python", "s3upload.py"]