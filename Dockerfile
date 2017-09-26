FROM maven:latest
ADD . /usr/src/mymaven
WORKDIR  /usr/src/mymaven
RUN mvn dependency:resolve
RUN mvn compile
CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
