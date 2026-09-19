#!/usr/bin/env bash
# ============================================================
# One-time setup: create the Secrets Manager secret that
# download_api.py::get_secret() reads (username/password for the
# MDM Hub, iqvia_username/iqvia_password for IQVIA).
#
# Usage: ./create_secret.sh <secret-name> <region>
# Example: ./create_secret.sh healthcare-mdm/dev/api-credentials us-east-1
# ============================================================
set -euo pipefail

SECRET_NAME="${1:-healthcare-mdm/dev/api-credentials}"
REGION="${2:-us-east-1}"

read -rp "MDM Hub username: " MDM_HUB_USERNAME
read -rsp "MDM Hub password: " MDM_HUB_PASSWORD
echo
read -rp "IQVIA username (or your mock API's dummy value): " IQVIA_USERNAME
read -rsp "IQVIA password (or your mock API's dummy value): " IQVIA_PASSWORD
echo

aws secretsmanager create-secret \
    --name "$SECRET_NAME" \
    --region "$REGION" \
    --secret-string "$(cat <<JSON
{
  "username": "$MDM_HUB_USERNAME",
  "password": "$MDM_HUB_PASSWORD",
  "iqvia_username": "$IQVIA_USERNAME",
  "iqvia_password": "$IQVIA_PASSWORD"
}
JSON
)" \
  || aws secretsmanager update-secret \
       --secret-id "$SECRET_NAME" \
       --region "$REGION" \
       --secret-string "$(cat <<JSON
{
  "username": "$MDM_HUB_USERNAME",
  "password": "$MDM_HUB_PASSWORD",
  "iqvia_username": "$IQVIA_USERNAME",
  "iqvia_password": "$IQVIA_PASSWORD"
}
JSON
)"

echo "Secret '$SECRET_NAME' created/updated in $REGION."
