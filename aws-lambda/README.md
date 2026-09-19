# Deploying `download_api.py` as a Real AWS Lambda

`src/api/download_api.py` already contains a real, working
`lambda_handler(event, context)` function - it was written to be an AWS
Lambda from the start (see `download_api.docx`: *"this is an AWS Lambda
API that acts as a bridge between IQVIA and Kokoro/Informatica MDM"*).
Nothing in that file needs to change. This folder only adds the
deployment plumbing around it.

**Good news for you specifically:** `download_api.py` has zero
dependencies on any other file in this repo (no `from .sbc import ...`
etc.) - it only needs `boto3` (built into every Lambda runtime already)
and `requests` (the one thing we bundle). That makes this the simplest
possible Lambda to deploy.

## Prerequisites (one-time, on your own machine)

1. **AWS CLI** installed and configured: `aws configure` (needs an AWS
   account - free tier covers this comfortably; Lambda's free tier alone
   is 1M requests/month).
2. **AWS SAM CLI** installed (`pip install aws-sam-cli` or see
   https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
   - SAM is AWS's own tool for exactly this: package + deploy a Lambda +
     API Gateway + IAM role together from one template, instead of
     clicking through the Console by hand.
3. **Python 3.12** locally (to match the Lambda runtime).

## Step-by-step

### Step 1 - Decide what `iqvia_url` and `base_MDM_HUB_url` point to
- For testing right now (no real IQVIA credentials): point `IqviaUrl` at
  your own deployed mock API (see the earlier `api/` FastAPI mock, or
  wherever you host it - Render.com, an EC2 box, etc).
- For `MdmHubUrl`: same idea - point it at wherever your Informatica MDM
  Hub / Kokoro test environment lives, or another mock, until real
  credentials exist.

### Step 2 - Create the Secrets Manager secret
```bash
cd aws-lambda
./create_secret.sh healthcare-mdm/dev/api-credentials us-east-1
```
This prompts for 4 values and stores them as one JSON secret - exactly
the shape `download_api.py::load_runtime_credentials()` expects
(`username`, `password`, `iqvia_username`, `iqvia_password`). You can
type placeholder/fake values here for now; swap them for real ones later
with the same script (it updates in place if the secret already exists).

### Step 3 - Generate a Bearer API key
```bash
python3 -c "import secrets; print(secrets.token_hex(24))"
```
Save this value somewhere safe (e.g. a password manager) - you'll pass
it as a deploy parameter next, and use it in every request's
`Authorization: Bearer <this value>` header.

### Step 4 - Build the deployment package
```bash
./build_deployment_package.sh
```
This copies `download_api.py` and installs `requests` into
`aws-lambda/package/` - the exact folder `template.yaml` tells SAM to
package as the function's code.

### Step 5 - Deploy with SAM
```bash
sam build
sam deploy --guided
```
`--guided` walks you through an interactive first-time setup (stack
name, region, and it will prompt for `IqviaUrl`, `MdmHubUrl`, `SecretName`,
`ApiKey` - the four Parameters declared in `template.yaml`). It saves
your answers to `samconfig.toml` so future deploys are just `sam deploy`.

When it finishes, it prints an **Outputs** section with `ApiEndpoint` -
this is your real, public HTTPS URL, e.g.:
```
https://abc123xyz.execute-api.us-east-1.amazonaws.com/dev/hcp
```

### Step 6 - Test it
```bash
curl -X POST "https://abc123xyz.execute-api.us-east-1.amazonaws.com/dev/hcp" \
  -H "x-api-key: <the API Gateway key SAM created - see Step 7>" \
  -H "Authorization: Bearer <the ApiKey value you chose in Step 3>" \
  -H "Content-Type: application/json" \
  -d '{"mdmEntityType": "HCP", "iqviaId": "W12345678"}'
```
Or test the Lambda directly without going through API Gateway at all
(faster feedback loop while developing):
```bash
sam local invoke DownloadApiFunction --event test_event.json
```
(edit `test_event.json`'s `Authorization` value to match your real
`ApiKey` first.)

### Step 7 - Retrieve the API Gateway key value
`ApiKeyRequired: true` in `template.yaml` makes API Gateway generate its
own separate key (this is IN ADDITION to the Lambda's own Bearer check -
see the NOTE in `template.yaml` for why both exist). Get its value with:
```bash
aws apigateway get-api-keys --name-query healthcare-mdm --include-values
```

## Wiring this into the rest of the pipeline

Once deployed, put the URL + API key where `src/ingestion/` and any
other caller can read them - the same pattern `download_api.py` itself
already uses for its OWN secrets:

```python
import boto3, json
client = boto3.client("secretsmanager", region_name="us-east-1")
creds = json.loads(
    client.get_secret_value(SecretId="healthcare-mdm/dev/download-api-invoker")["SecretString"]
)
# creds = {"url": "https://abc123xyz.execute-api.../dev/hcp", "api_key": "..."}
```
Store a *second*, small secret like the one above (URL + API key pair)
separately from the `create_secret.sh` one (which holds the
MDM-Hub/IQVIA credentials `download_api.py` itself needs) - keep the
"credentials this Lambda needs to call out" and "credentials others need
to call this Lambda" as two distinct secrets, so rotating one never
touches the other.

## Updating the code later
Whenever `src/api/download_api.py` changes:
```bash
git pull                        # get the latest code
cd aws-lambda
./build_deployment_package.sh   # rebuild package/ with the new code
sam build && sam deploy         # redeploy (no --guided needed after the first time)
```
This is the "git push -> redeploy" half of the CODE vs DATA distinction
discussed earlier - only needed when the *code* changes, never for
individual HCP/HCO lookups (those are just HTTPS calls to the already-
deployed endpoint, any time, from anywhere, with no git involved).
