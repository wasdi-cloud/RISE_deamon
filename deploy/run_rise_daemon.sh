  GNU nano 4.8                                                                          /opt/wasdi/docker/wasdi-trigger/run.sh
#!/bin/bash

sInstanceTime="$(date '+%Y-%m-%d - %H:%M:%S')"

echo "#### BEGIN ${sInstanceTime} ####"

if [[ -z "${USER}" ]]
then
    echo "[DEBUG] The variable '\${USER}' is empty: we fill it manually with the command 'whoami'"
    USER="$(whoami)"
fi

if [[ -z "${USER}" ]]
then
    echo "[ERROR] The variable '\${USER}' is still empty: we exit here"
    echo "#### END ${sInstanceTime} ####"
    exit 1
fi

if [[ "${USER}" != "appwasdi" ]]
then
    echo "[ERROR] You are '${USER}' when should be 'appwasdi'"
    echo "#### END ${sInstanceTime} ####"
    exit 1
fi

echo "\${USER} = ${USER}"
echo "\${@} = ${@}"

docker \
    run \
    --mount type=bind,source=/etc/rise,destination=/etc/rise,readonly \
    --network net-wasdi \
    rise-deamon:latest

echo "#### END ${sInstanceTime} ####"



