#!/bin/bash

COMPOSE="docker-compose"
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

check=$( which ${COMPOSE} 2>/dev/null >/dev/null )
if [ "$?" == "1" ]
then
    COMPOSE="$DIR/bin/${COMPOSE}"
    if [ ! -f ${COMPOSE} ]
    then
        if [ ! -d "$DIR/bin/" ]
        then
            mkdir "$DIR/bin/"
        fi

        echo "install ${COMPOSE} localy"
        curl -L https://github.com/docker/compose/releases/download/1.2.0/docker-compose-`uname -s`-`uname -m` > ${COMPOSE}
        chmod +x ${COMPOSE}
    fi
fi
${COMPOSE} --version


