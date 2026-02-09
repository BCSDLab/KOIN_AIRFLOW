param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("workbook", "datasource")]
    [string]$Target,
    [Parameter(Mandatory = $false)]
    [string]$ProjectId = "kap-chat",
    [Parameter(Mandatory = $false)]
    [string]$Location = "asia-northeast3",
    [Parameter(Mandatory = $false)]
    [string]$EnvName = "airflow-prod",
    [Parameter(Mandatory = $false)]
    [bool]$DebugAuthOnly = $true
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

gcloud config set project $ProjectId | Out-Null

Write-Host "Loading secrets from Secret Manager..."
$TABLEAU_SERVER_URL = gcloud secrets versions access latest --secret=tableau-server-url
$TABLEAU_SITE_CONTENT_URL = gcloud secrets versions access latest --secret=tableau-site-content-url
$TABLEAU_PAT_NAME = gcloud secrets versions access latest --secret=tableau-pat-name
$TABLEAU_PAT_SECRET = gcloud secrets versions access latest --secret=tableau-pat-secret

$envVars = @(
    "TABLEAU_SERVER_URL=$TABLEAU_SERVER_URL",
    "TABLEAU_SITE_CONTENT_URL=$TABLEAU_SITE_CONTENT_URL",
    "TABLEAU_PAT_NAME=$TABLEAU_PAT_NAME",
    "TABLEAU_PAT_SECRET=$TABLEAU_PAT_SECRET",
    "TABLEAU_API_VERSION=3.27"
)

if ($Target -eq "workbook") {
    $TABLEAU_WORKBOOK_ID = gcloud secrets versions access latest --secret=tableau-workbook-id
    $envVars += "TABLEAU_REFRESH_TARGET=workbook"
    $envVars += "TABLEAU_WORKBOOK_ID=$TABLEAU_WORKBOOK_ID"
} else {
    $TABLEAU_DATASOURCE_ID = gcloud secrets versions access latest --secret=tableau-datasource-id
    $envVars += "TABLEAU_REFRESH_TARGET=datasource"
    $envVars += "TABLEAU_DATASOURCE_ID=$TABLEAU_DATASOURCE_ID"
}

$envVars += "TABLEAU_DEBUG_AUTH_ONLY=$($DebugAuthOnly.ToString().ToLower())"

Write-Host "Updating Composer environment variables..."
gcloud composer environments update $EnvName `
  --location $Location `
  --update-env-variables ($envVars -join ",")

Write-Host "Done."
