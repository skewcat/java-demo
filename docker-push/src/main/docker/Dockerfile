FROM harbor.mig.ruijie.net/rgibns-snapshot/ml-center:1.0

MAINTAINER RuijieML
USER root

ADD py_code/ /home/ibns/code

ENV BOOTSTRAP /etc/bootstrap.sh
ENTRYPOINT ["/etc/bootstrap.sh"]