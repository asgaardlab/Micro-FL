FROM python:3.11
RUN pip install --upgrade pip
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN apt-get update && apt-get upgrade -y && apt-get install gcc -y
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# CMD [ "python", "app.py" ]
CMD ["gunicorn", "--bind", ":8000", "app:app"]
