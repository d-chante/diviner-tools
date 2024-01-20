FROM ubuntu:22.04

RUN apt-get update && apt upgrade -y
RUN apt-get install python3-pip -y

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN mkdir -p /container/env
COPY ../other/requirements.txt /container/env/requirements.txt

RUN pip install --no-cache-dir -r /container/env/requirements.txt

RUN mkdir /diviner-tools
WORKDIR /diviner-tools

ENTRYPOINT ["/entrypoint.sh"]