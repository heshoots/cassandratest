FROM maven:latest
ADD . /usr/src/mymaven
WORKDIR  /usr/src/mymaven
RUN mvn dependency:resolve
RUN mvn compile
CMD ["mvn", "exec:java"]
