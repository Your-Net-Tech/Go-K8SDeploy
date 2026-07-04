#!/bin/bash

# Playback script for Go-K8SDeploy demonstration
# Designed to be recorded with asciinema or screen capture

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m' # No Color

type_cmd() {
    local cmd="$1"
    echo -ne "${GREEN}jrol@k8s-server:/opt/go-k8sdeploy$ ${NC}"
    for (( i=0; i<${#cmd}; i++ )); do
        echo -n "${cmd:$i:1}"
        sleep 0.04
    done
    echo ""
    sleep 0.3
    eval "$cmd"
    echo ""
    sleep 1.2
}

clear
echo -e "${BLUE}=========================================================================${NC}"
echo -e "${CYAN}             Go-K8SDeploy - Native Kubernetes Orcherstration Engine      ${NC}"
echo -e "${BLUE}=========================================================================${NC}"
echo -e "Developed by: ${YELLOW}Your Net Tec${NC} | License: ${GREEN}AGPL${NC}"
echo -e "Starting demonstration playback (Real GitOps Drift & Reconcile)..."
echo ""
sleep 2

type_cmd "./bin/go-k8sdeploy --help"

type_cmd "cat config-demo.yaml"

# Cleanup previous namespaces to show a fresh execution
type_cmd "kubectl delete ns k8s-demo --ignore-not-found=true"

# 1. Run actual deployment
type_cmd "./bin/go-k8sdeploy apply -p k8s-demo -c config-demo.yaml"

# 2. Verify resources in cluster
type_cmd "kubectl get pods -n k8s-demo"

# 3. Simulate a manual modification / Configuration Drift in the cluster
echo -e "${YELLOW}[DRIFT] Simulating a manual change in cluster (modifying Nginx image directly)...${NC}"
type_cmd "kubectl patch pod nginx-demo -n k8s-demo --type='json' -p='[{\"op\": \"replace\", \"path\": \"/spec/containers/0/image\", \"value\": \"nginx:latest\"}]'"

# 4. Run drift detection to reveal the configuration drift instantly
type_cmd "./bin/go-k8sdeploy drift -p k8s-demo -c config-demo.yaml"

# 5. Automatically reconcile / self-heal the drift by re-applying the config
echo -e "${GREEN}[RECONCILE] Re-applying config to automatically heal the drift...${NC}"
type_cmd "./bin/go-k8sdeploy apply -p k8s-demo -c config-demo.yaml"

# 6. Verify drift is now fully resolved
type_cmd "./bin/go-k8sdeploy drift -p k8s-demo -c config-demo.yaml"

# 7. Check deployment status/revision history
type_cmd "./bin/go-k8sdeploy status -p k8s-demo"

# 8. Run local stress benchmark
type_cmd "./bin/go-k8sdeploy bench -s stress"

echo -e "${BLUE}=========================================================================${NC}"
echo -e "${CYAN}             Demonstration Complete! Go-K8SDeploy is 100% operational.   ${NC}"
echo -e "${BLUE}=========================================================================${NC}"
echo ""
