#!/usr/bin/env bash

echo "=== HDFS NameNode Failover Simulation ==="

echo "Initial State:"
echo "NN1: $(hdfs haadmin -getServiceState nn1 2>/dev/null || echo 'N/A')"
echo "NN2: $(hdfs haadmin -getServiceState nn2 2>/dev/null || echo 'N/A')"

echo "Triggering manual state transition..."
hdfs haadmin -transitionToStandby --forcemanual nn1 || true
hdfs haadmin -transitionToActive --forcemanual nn2 || true

echo "Post-transition State:"
echo "NN1: $(hdfs haadmin -getServiceState nn1 2>/dev/null || echo 'N/A')"
echo "NN2: $(hdfs haadmin -getServiceState nn2 2>/dev/null || echo 'N/A')"
