#!/bin/zsh


docker run -ti -v /Users/luxu/Documents/.IdeaProjects/bdat:/home/ -p 1688:1688 --name bdtaimage --privileged=true  python /sbin/init
docker run -ti -v /Users/luxu/Documents/.IdeaProjects/bdat:/home/ --name centos7 --privileged=true  couchbase/centos7-systemd /sbin/init


