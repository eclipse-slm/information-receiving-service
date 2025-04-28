# Information Receiving Service (IRS)

Service for aggregating remote Asset Administration Shells (AAS) published by public AAS API or via EDC Dataspace connectors.

## Install

### Create Config File

Create config file "config.yml" and place in same folder as docker-compose.yml. Add remote AASX Servers as follows:

```yaml
# config.yml
aas-servers:
  - name: "AASX Server 1"                           # choose as you like
    url: "https://example-1.com/aasx-server-1"      # base URL of AASX Server
    polling_interval_s: 60                          # interval in seconds for polling the server (optional, default: 60 s)
    auth:
      auth-type: oauth2 | apikey | custom-oauth     # chose one of the auth types
      # if oauth2:
      client-id: "your-client-id"                   # client id for oauth2
      client-secret: "your-client-secret"           # client secret for oauth2
      token-url: "https://example-1.com/token"      # token URL for oauth2
      # if apikey:
      api-key: "your-api-key"                       # API key for AASX Server
      # if custom-oauth:
      client-id: "your-client-id"                   # client id for custom oauth
      secret: "your-secret"                         # secret for custom oauth
      login-url: "https://example-1.com/login"      # login URL for custom oauth
  - name: "AASX Server 2"
    url: "https://example-2.com/aasx-server-2"
    auth:
      # see example 1 from above
      ...
  - name: "AAS Services 1"
    urls:
      shell-registry: "https://shell-registry.local"            # base URL of (remote) Shell Registry
      shell-repository: "https://shell-repository.local"        # base URL of (remote) Shell Repository
      submodel-registry: "https://submodel-registry.local"      # base URL of (remote) Submodel Registry
      submodel-repository: "https://submodel-repository.local"  # base URL of (remote) Submodel Repository
```

### Start IRS (Information Receiving Service)

Use docker-compose.yml in root folder of this project to start the IRS:

```bash
docker-compose up -d
```

### Use IRS

IRS will start to cache the content of the AASX Servers in the background. You can check the status of the IRS by opening the following URL in your browser:

```http request
http://localhost:5984/_utils

# default credentials:
# username: username
# password: password
```

To consume local cached content of remote AASX Servers you can use the API of IRS. The documentation of the API is available at:

```http request
http://localhost:3000/docs
```

### Stop IRS

To stop the IRS, use the following command:

```bash
docker-compose down
```
