FROM registry.opensource.zalan.do/stups/python:3.5.0-2
MAINTAINER team-saiki@zalando.de

ENV ZOOKEEPER_NAMESPACE_SAIKI=/saiki
ENV ZOOKEEPER_NAMESPACE_KAFKA=/buku
ENV ZOOKEEPER_NAMESPACE_PEMETAAN=/pemetaan
ENV ZOOKEEPER_NAMESPACE_MANGAN=/mangan

# needed for uwsgi_metrics (treap module)
RUN apt-get update
RUN apt-get install m4 wget -q -y

COPY requirements.txt /
RUN pip3 install -r /requirements.txt

RUN apt-key adv --keyserver hkp://pgp.mit.edu:80 --recv-keys 573BFD6B3D8FBC641079A6ABABF5BD827BD9BF62
RUN echo "deb http://nginx.org/packages/mainline/debian/ jessie nginx" >> /etc/apt/sources.list

RUN apt-get install -y ca-certificates nginx && \
    rm -rf /var/lib/apt/lists/*

# forward request and error logs to docker log collector
RUN ln -sf /dev/stdout /var/log/nginx/access.log
RUN ln -sf /dev/stderr /var/log/nginx/error.log

COPY pemetaan_nginx.conf /etc/nginx/sites-enabled/pemetaan_nginx.conf
COPY nginx.conf /etc/nginx/nginx.conf

COPY start_uwsgi_and_nginx.sh /tmp/start_uwsgi_and_nginx.sh
RUN chmod 777 /tmp/start_uwsgi_and_nginx.sh

RUN mkdir /etc/nginx/ssl
COPY ssl_certs/* /etc/nginx/ssl/

RUN mkdir -p /opt/pemetaan/
COPY app /opt/pemetaan

RUN wget -O /opt/pemetaan/rebalance_partitions.py https://raw.githubusercontent.com/zalando/saiki-buku/e2799ab3b21aace117a1fd5b9b784535a2d2ba30/rebalance_partitions.py
RUN wget -O /opt/pemetaan/static/jquery.validate-json.js https://raw.githubusercontent.com/dustinboston/validate-json/master/jquery.validate-json.js

COPY scm-source.json /scm-source.json

WORKDIR /opt/pemetaan

CMD /tmp/start_uwsgi_and_nginx.sh

EXPOSE 80 443
