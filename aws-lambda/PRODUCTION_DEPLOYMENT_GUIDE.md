# Production-Style AWS Lambda Deployment - Full Console Walkthrough

This is the AWS Console (point-and-click) version of the same deployment
`template.yaml`/SAM automates. Do this once by hand to understand every
moving part; use SAM (`README.md` in this folder) for repeat deploys.

## PART 1: IAM Role (permissions the Lambda runs with)

1. AWS Console -> **IAM** -> **Roles** -> **Create role**.
2. Trusted entity type: **AWS service** -> Use case: **Lambda** -> Next.
3. Skip attaching a managed policy for now (we'll attach a scoped one) -> Next.
4. Role name: `healthcare-mdm-download-api-role-dev` -> Create role.
5. Open the role -> **Add permissions** -> **Create inline policy** -> JSON tab -> paste:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": "secretsmanager:GetSecretValue",
         "Resource": "arn:aws:secretsmanager:us-east-1:<YOUR_ACCOUNT_ID>:secret:healthcare-mdm/dev/api-credentials*"
       },
       {
         "Effect": "Allow",
         "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
         "Resource": "arn:aws:logs:us-east-1:<YOUR_ACCOUNT_ID>:*"
       }
     ]
   }
   ```
   - **Why scoped like this (production practice):** the Secrets Manager
     permission only allows reading ONE specific secret ARN, not
     `"Resource": "*"` - if this Lambda is ever compromised, it cannot
     read any other secret in the account. The CloudWatch Logs
     permissions are what let the Lambda write its `logger.info(...)`
     output somewhere you can see it.
   - Name the policy `download-api-secrets-read` -> Create policy.

## PART 2: Secrets Manager (store IQVIA + MDM Hub credentials)

1. Console -> **Secrets Manager** -> **Store a new secret**.
2. Secret type: **Other type of secret**.
3. Key/value pairs (switch to key/value view):
   | Key | Value (example) |
   |---|---|
   | `username` | your MDM Hub username (or a fake placeholder for now) |
   | `password` | your MDM Hub password |
   | `iqvia_username` | your IQVIA username (or your mock API's dummy value) |
   | `iqvia_password` | your IQVIA password (or your mock API's dummy value) |
4. Encryption key: leave the default `aws/secretsmanager` (fine for dev; a
   customer-managed KMS key is a production hardening option, not
   required to get started).
5. Secret name: `healthcare-mdm/dev/api-credentials` (must match exactly
   what you put in `SECRET_NAME`/`secret_name` env var later).
6. Next -> Next (skip automatic rotation for now - see Part 6 for why
   you'd turn it on later) -> Store.
7. Open the secret afterwards and copy its **ARN** - you'll need it for
   the IAM policy above if you didn't already know the account ID.

## PART 3: Create the Lambda function itself

1. Console -> **Lambda** -> **Create function**.
2. **Author from scratch**.
3. Function name: `healthcare-mdm-download-api-dev`.
4. Runtime: **Python 3.12**.
5. Architecture: `x86_64` (default is fine).
6. **Change default execution role** -> **Use an existing role** -> pick
   `healthcare-mdm-download-api-role-dev` from Part 1.
7. Create function.

## PART 4: Upload the code

1. Locally, run `aws-lambda/build_deployment_package.sh` (this puts
   `download_api.py` + the `requests` library into `aws-lambda/package/`).
2. Zip that folder's *contents* (not the folder itself) into
   `download_api.zip`:
   ```bash
   cd aws-lambda/package && zip -r ../download_api.zip . && cd ..
   ```
3. In the Lambda console -> **Code** tab -> **Upload from** -> **.zip file**
   -> select `download_api.zip` -> Save.
4. **Runtime settings** -> **Edit** -> Handler: `download_api.lambda_handler`
   (module name `download_api`, function name `lambda_handler` - this
   MUST match exactly, since AWS Lambda finds your entry point by this
   string).

## PART 5: Environment variables

Lambda console -> **Configuration** tab -> **Environment variables** -> Edit -> Add:

| Key | Value |
|---|---|
| `api_key` | the Bearer token you generated (`python3 -c "import secrets; print(secrets.token_hex(24))"`) |
| `base_MDM_HUB_url` | your MDM Hub / Kokoro base URL (or mock API URL for now) |
| `iqvia_url` | your IQVIA base URL (or mock API URL for now) |
| `region` | `us-east-1` (or your region) |
| `secret_name` | `healthcare-mdm/dev/api-credentials` |

**Production note:** never put the real `api_key` value directly as a
plain environment variable in a real production account long-term -
Lambda environment variables ARE encrypted at rest, but anyone with
`lambda:GetFunctionConfiguration` IAM permission can read them in
plaintext via the console/CLI. For production, store `api_key` in
Secrets Manager too (a second secret) and read it inside
`load_runtime_credentials()` the same way `MDM_HUB_USERNAME` etc. are
already read - this repo's current code takes the env-var shortcut,
which is fine for dev/test.

Also bump **Configuration -> General configuration -> Timeout** to 30
seconds and **Memory** to 256 MB (the defaults of 3 sec / 128 MB are too
tight for an HTTP-calling function with retries).

## PART 6: API Gateway (expose it over HTTPS, with an API key)

1. Console -> **API Gateway** -> **Create API** -> **REST API** (not
   HTTP API - REST API has the built-in API-key/usage-plan feature this
   design relies on) -> Build.
2. API name: `healthcare-mdm-download-api`.
3. **Actions -> Create Resource** -> Resource name: `hcp` -> Create.
4. With `/hcp` selected -> **Actions -> Create Method** -> `POST`.
5. Integration type: **Lambda Function** -> check **Use Lambda Proxy
   integration** (this is what makes `event["body"]`/`event["headers"]`
   show up exactly the way `lambda_handler` expects) -> pick your
   function -> Save -> OK (grants API Gateway permission to invoke it).
6. **Method Request** -> set **API Key Required** to `true`.
7. **Actions -> Deploy API** -> New stage -> name it `dev` -> Deploy.
   Note the **Invoke URL** shown (e.g.
   `https://abc123xyz.execute-api.us-east-1.amazonaws.com/dev`).
8. Create the actual key: **API Keys** (left nav) -> **Create API key**
   -> name it, save.
9. **Usage Plans** -> **Create** -> set a rate limit (e.g. 10 req/sec,
   burst 20 - stops a bug in a caller from running up your AWS bill) ->
   **Add API Stage** (pick `dev`) -> **Add API Key** (pick the key from
   step 8).

## PART 7: Test it end-to-end

```bash
curl -X POST "https://abc123xyz.execute-api.us-east-1.amazonaws.com/dev/hcp" \
  -H "x-api-key: <API Gateway key value from Part 6 step 8>" \
  -H "Authorization: Bearer <api_key env var value from Part 5>" \
  -H "Content-Type: application/json" \
  -d '{"mdmEntityType": "HCP", "iqviaId": "W12345678"}'
```
Two separate keys are checked here on purpose:
- `x-api-key` -> API Gateway's own check (rejects traffic before it ever
  reaches your Lambda - saves invocation cost on junk/bot traffic).
- `Authorization: Bearer ...` -> the Lambda's own `authenticate_request()`
  check (matches the real IQVIA-side contract exactly, so this code
  behaves identically whether called through API Gateway or invoked any
  other way, e.g. Lambda-to-Lambda).

## PART 8: Watching it run (CloudWatch)

Lambda console -> **Monitor** tab -> **View CloudWatch logs**. Every
`logger.info(...)` / `logger.error(...)` line in `download_api.py` shows
up here per invocation - this is your primary debugging tool once it's
deployed (no more "print and rerun locally").

## PART 9: Production hardening checklist (do these before it's "real" production)

- [ ] Move `api_key` out of plain environment variables into Secrets Manager (Part 5 note).
- [ ] Turn on **automatic rotation** for the Secrets Manager secret (Secrets Manager console -> your secret -> Rotation -> needs a small rotation Lambda AWS can auto-generate for you).
- [ ] Set a **CloudWatch Alarm** on the Lambda's `Errors` metric (Lambda console -> Monitor -> Alarms -> Create alarm) so failures page someone instead of sitting silently in logs.
- [ ] Put the Lambda in a **VPC** if `base_MDM_HUB_url` is only reachable over a private network (your company's VPN/Direct Connect) - Configuration -> VPC.
- [ ] Set **Reserved concurrency** on the function so a traffic spike can't consume your account's entire concurrent-execution limit and starve other Lambdas.
- [ ] Create separate secrets/functions/API Gateway stages per environment (`dev`, `test`, `prod`) - never point the `prod` stage at dev credentials.
- [ ] Enable **AWS X-Ray tracing** (Configuration -> Monitoring and operations tools) for request-level tracing across API Gateway -> Lambda -> the outbound IQVIA/MDM Hub calls.
