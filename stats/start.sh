#!/bin/bash
/usr/local/bin/prometheus --config.file=/etc/prometheus/prometheus.yml &
/usr/local/bin/pushgateway &
/grafana-7.4.1/bin/grafana-server --config=/etc/grafana/grafana.ini &