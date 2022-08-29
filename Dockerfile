FROM alpine:3.16
RUN echo "https://dl-cdn.alpinelinux.org/alpine/v3.16/main" >> /etc/apk/repositories
RUN echo "https://dl-cdn.alpinelinux.org/alpine/v3.16/community" >> /etc/apk/repositories
RUN sed -i "s/999/99/" /etc/group  #ChangedPingTo99
RUN apk --update add --no-cache ca-certificates tzdata bash git tini-static python3 py3-pip py3-requests openssl curl docker docker-py
RUN apk --update add --no-cache --virtual buildenv python3-dev openssl-dev build-base
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN pip3 install --upgrade --ignore-installed "setuptools>=40.8" "poetry>=1.1.15"
RUN addgroup -g 999 -S debiandocker && addgroup -g 998 -S ubuntudocker && addgroup -g 978 -S fedoradocker
RUN adduser ofelia --disabled-password --shell /bin/bash &&\
    addgroup ofelia users && addgroup ofelia docker &&\
    addgroup ofelia debiandocker && addgroup ofelia ubuntudocker && addgroup ofelia fedoradocker
RUN mkdir -p /usr/local/app && chown -R ofelia:users /usr/local/app
WORKDIR /usr/local/app
#USER ofelia
RUN python3 -m venv --system-site-packages --symlinks .venv
ADD pyproject.toml poetry.lock poetry.toml ./
ADD pyofelia ./pyofelia
RUN poetry install -v
#USER root
RUN apk del buildenv
# Add a daemon executable file to backwards compatibility with the Go implementation of Ofelia
ADD ./ofelia.sh .
RUN chmod a+x ./ofelia.sh
RUN ln -s /usr/local/app/ofelia.sh /usr/bin/daemon
#USER ofelia
ENTRYPOINT ["/sbin/tini-static", "--"]
CMD ["daemon"]
