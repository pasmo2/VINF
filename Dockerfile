FROM coady/pylucene
WORKDIR /usr/src/app
COPY . .
CMD ["python3", "-m", "http.server", "80"]