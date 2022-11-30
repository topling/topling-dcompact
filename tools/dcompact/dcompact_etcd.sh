#!/bin/sh

#export WORKER_DB_NAME=/tmp/dcompact_worker-$$
export WORKER_ROOT=db_dist_compact
export HOSTER_ROOT=db_dist_compact
export DEL_WORKER_TEMP_DB=false
export TerarkZipTable_localTempDir=/dev/shm/dcompact-$$
mkdir -p ${TerarkZipTable_localTempDir}

dir=$(realpath $(dirname $0))

#set -x; cat > /tmp/compact_rpc_params-$$.bin; exit 1
#VARIANT=${VARIANT:-dbg}
VARIANT=${VARIANT:-rls}
#exe=/nvme1/leipeng/osc/topling-rocks/tools/dcompact/build/Linux-x86_64-g++-8.3-bmi2-1/${VARIANT}/dcompact_worker.exe
exe=$dir/build/Linux-x86_64-g++-8.3-bmi2-1/${VARIANT}/dcompact_worker.exe
#rm   -rf ${WORKER_DB_NAME}
#mkdir -p ${WORKER_DB_NAME}

${exe} # run it
