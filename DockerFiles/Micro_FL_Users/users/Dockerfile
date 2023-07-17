FROM python:3.9
RUN pip install --upgrade pip
WORKDIR /usr/src/users
COPY requirements.txt ./
RUN apt-get update && apt-get upgrade -y && apt-get install gcc -y
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD [ "python", "users.py" ]
