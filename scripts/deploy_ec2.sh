#!/usr/bin/env bash
set -euo pipefail

KEY_PATH="${DEPLOY_SSH_KEY:-/Users/firstquarter/Downloads/boxer.pem}"
SSH_USER="${DEPLOY_SSH_USER:-ec2-user}"
SSH_HOST="${DEPLOY_SSH_HOST:-43.203.174.230}"
REMOTE_DIR="${DEPLOY_REMOTE_DIR:-/home/ec2-user/rag-bot}"
BRANCH="${DEPLOY_BRANCH:-main}"
SERVICE_NAME="${DEPLOY_SERVICE_NAME:-boxer}"
SYNC_ENV="${DEPLOY_SYNC_ENV:-true}"
LOCAL_ENV_PATH="${DEPLOY_LOCAL_ENV_PATH:-.env}"

if [[ ! -f "${KEY_PATH}" ]]; then
  echo "SSH key file not found: ${KEY_PATH}" >&2
  exit 1
fi

echo "[deploy] target=${SSH_USER}@${SSH_HOST}"
echo "[deploy] remote_dir=${REMOTE_DIR} branch=${BRANCH} service=${SERVICE_NAME}"

if [[ "${SYNC_ENV}" == "true" ]]; then
  if [[ ! -f "${LOCAL_ENV_PATH}" ]]; then
    echo "Local env file not found: ${LOCAL_ENV_PATH}" >&2
    exit 1
  fi
  echo "[deploy] syncing env file ${LOCAL_ENV_PATH} -> ${REMOTE_DIR}/.env"
  ssh -i "${KEY_PATH}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${SSH_HOST}" \
    "if [ -f '${REMOTE_DIR}/.env' ]; then cp '${REMOTE_DIR}/.env' '${REMOTE_DIR}/.env.bak-\$(date +%Y%m%d-%H%M%S)'; fi"
  scp -i "${KEY_PATH}" -o StrictHostKeyChecking=accept-new "${LOCAL_ENV_PATH}" \
    "${SSH_USER}@${SSH_HOST}:${REMOTE_DIR}/.env"
fi

ssh -i "${KEY_PATH}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${SSH_HOST}" <<EOF
set -euo pipefail
cd "${REMOTE_DIR}"
chmod 600 .env
git pull origin "${BRANCH}"
source .venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart "${SERVICE_NAME}"
sudo systemctl status "${SERVICE_NAME}" --no-pager -l | head -n 25
sudo journalctl -u "${SERVICE_NAME}" -n 20 --no-pager -o short-iso
EOF

echo "[deploy] done"
