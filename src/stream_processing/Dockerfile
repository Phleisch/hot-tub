FROM openjdk:8

# Environment
ENV SBT_VERSION 1.3.13

# Install SBT
RUN curl -L -o sbt-$SBT_VERSION.deb http://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
    dpkg -i sbt-$SBT_VERSION.deb && \
    rm sbt-$SBT_VERSION.deb && \
    apt-get update && \
    apt-get install sbt

# Compile application
COPY StreamProcessing.scala /usr/src/app/StreamProcessing.scala
COPY build.sbt /usr/src/app/build.sbt
WORKDIR /usr/src/app
RUN sbt compile
# Start streaming application
CMD sbt run
