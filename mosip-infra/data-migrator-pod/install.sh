#!/bin/bash
# Installs sample mpesa service
## Usage: ./restart.sh [kubeconfig]

if [ $# -ge 1 ] ; then
  export KUBECONFIG=$1
fi

NS=data-migrator
CHART_VERSION=12.0.1

echo Create $NS namespace
kubectl create ns $NS 

function installing_migrator() {
  echo Istio label
  kubectl label ns $NS istio-injection=enabled --overwrite
  helm repo update
  helm dependency update helm/

DATABASE_IP_ADDRESS=

echo Installing Migrator

#  echo Copy Configmaps
#  sed -i 's/\r$//' copy_cm.sh
#  ./copy_cm.sh

#  echo Copy Secrets
#  sed -i 's/\r$//' copy_secret.sh
#  ./copy_secret.sh

#  echo Creating Database in Postgres mosip_mpesa
#  ./postgres-init-db/install.sh

  kubectl delete configmap property-config -n $NS --ignore-not-found=true
  kubectl create configmap property-config  \
    --from-file=../../packet-generator/data-extractor-conversion/src/main/resources/externalsamples/ApiRequest.json \
    --from-file=../../packet-generator/data-extractor-conversion/src/main/resources/externalsamples/application-default.properties \
    --from-file=../../packet-generator/data-extractor-conversion/src/main/resources/externalsamples/BioFile.mvel \
    --from-file=../../packet-generator/data-extractor-conversion/src/main/resources/externalsamples/external_db.sql \
    --from-file=../../packet-generator/data-extractor-conversion/src/main/resources/externalsamples/identity.json \
    --from-file=../../packet-generator/data-extractor-conversion/src/main/resources/externalsamples/idschema.json -n $NS

  echo Installing data-migrator service
  helm -n $NS install data-migrator helm/ --wait --version $CHART_VERSION -f migrator.yaml
  return 0
}

# set commands for error handling.
set -e
set -o errexit   ## set -e : exit the script if any statement returns a non-true return value
set -o nounset   ## set -u : exit the script if you try to use an uninitialised variable
set -o errtrace  # trace ERR through 'time command' and other functions
set -o pipefail  # trace ERR through pipes
installing_migrator   # calling function
