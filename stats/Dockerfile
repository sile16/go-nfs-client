FROM prom/prometheus:v2.26.0
COPY prometheus.yml /etc/prometheus/

# Add pushgateway
RUN wget https://github.com/prometheus/pushgateway/releases/download/v1.1.0/pushgateway-1.1.0.linux-amd64.tar.gz
RUN tar xvzf pushgateway-1.1.0.linux-amd64.tar.gz
RUN mv pushgateway-1.1.0.linux-amd64 /usr/local/bin/pushgateway

#Add Grafana
RUN wget https://dl.grafana.com/oss/release/grafana-7.4.1.linux-amd64.tar.gz
RUN tar -zxvf grafana-7.4.1.linux-amd64.tar.gz

#Create Config
RUN mkdir /etc/grafana
COPY grafana.ini /etc/grafana/

# Add start script
COPY start.sh /
RUN chmod +x /start.sh

EXPOSE 9090
EXPOSE 9091
EXPOSE 3000

CMD ["./start.sh"]