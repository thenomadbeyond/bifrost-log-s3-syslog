FROM alpine:3.22

RUN apk add --no-cache rsyslog \
 && adduser -S -H -u 10001 rsyslog

USER 10001
EXPOSE 5514/tcp 5514/udp
ENTRYPOINT ["rsyslogd", "-n", "-f", "/etc/rsyslog/rsyslog.conf", "-i", "/tmp/rsyslogd.pid"]
