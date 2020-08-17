#!/bin/bash

cat <<EOF > /opt/drill/conf/drill-override.conf
drill.exec: {
  cluster-id: "drillbits1",
  zk.connect: "$DRILL_ZK_CLUSTER"
}
EOF

cleanup() {
  /opt/drill/bin/drillbit.sh graceful_stop
}

trap 'cleanup' SIGTERM

/opt/drill/bin/drillbit.sh --config /opt/drill/conf start

tail -f /opt/drill/log/drillbit.log
