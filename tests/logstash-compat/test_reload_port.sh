#!/bin/bash
# Test: reload closes old TCP port
CFG=/tmp/rel_port.conf
cat > "$CFG" <<EOF
input { tcp { port => "20001" } }
output { stdout { } }
EOF

~/logstash/bin/logstash -f "$CFG" --config.reload.automatic true --path.data /tmp/rel5_$$ > /tmp/rp.log 2>&1 &
PID=$!
sleep 2
echo "Port 20001 open (before):"
timeout 1 bash -c "</dev/tcp/localhost/20001" && echo YES || echo NO

# Trigger reload: change port to 20002
cat > "$CFG" <<EOF
input { tcp { port => "20002" } }
output { stdout { } }
EOF
sleep 5

echo "Port 20001 open (after reload):"
timeout 1 bash -c "</dev/tcp/localhost/20001" && echo YES || echo NO
echo "Port 20002 open (after reload):"
timeout 1 bash -c "</dev/tcp/localhost/20002" && echo YES || echo NO
echo "=== log tail ==="
tail -10 /tmp/rp.log

kill $PID 2>/dev/null
wait 2>/dev/null
rm -f "$CFG" /tmp/rp.log
