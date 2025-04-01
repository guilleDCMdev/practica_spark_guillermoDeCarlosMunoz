FROM debian:stable-slim

# Add Dependencies netcat
RUN apt-get update && apt-get install -y netcat-openbsd supervisor

COPY ./conf/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

COPY ./scripts/sendData.sh /
RUN sed -i 's/\r$//g' /sendData.sh
RUN chmod +x /sendData.sh

COPY ./scripts/startServer.sh /
RUN sed -i 's/\r$//g' /startServer.sh
RUN chmod +x /startServer.sh

CMD ["supervisord"]