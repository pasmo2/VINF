FROM coady/pylucene
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y default-jdk
RUN pip install pyspark==3.5.0 pandas
COPY . .
CMD ["python3", "-m", "http.server", "80"]
