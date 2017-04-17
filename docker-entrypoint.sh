#!/bin/sh

# Set Time Zone
cp /usr/share/zoneinfo/$TZ /etc/localtime
echo $TZ > /etc/TZ

# Start Services
java -jar target/cajrr-2.0-SNAPSHOT.jar server /etc/cajrr/config.yml
