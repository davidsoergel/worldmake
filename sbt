#!/bin/bash

declare jrebel="/home/saunders/projects/the-livingroom/app/jrebel/jrebel.jar"

# Jrebel version
## java -Xms512M -Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=384M -noverify -javaagent:$jrebel -jar `dirname $0`/sbt-launch-0.12.jar "$@"

java -Xms512M -Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=384M -jar `dirname $0`/sbt-launch-0.12.4.jar "$@"

