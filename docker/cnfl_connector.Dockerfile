FROM confluentinc/cp-server-connect-base:latest
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"
RUN confluent-hub install --no-prompt jcustenborder/kafka-connect-spooldir:2.0.43
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-mqtt:latest
# Additional connectors go below here