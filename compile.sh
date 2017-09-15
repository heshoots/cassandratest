docker run -it --rm  -v /root/.m2:/root/.m2  -v $(pwd):/opt/maven -w /opt/maven maven:3.2-jdk-7 mvn package
