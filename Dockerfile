FROM python:3.6.4

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY cellIdTracker-REST-API.py ./

CMD [ "gunicorn", "-b", "0.0.0.0:5000", "--preload", "cellIdTracker-REST-API:app" ]