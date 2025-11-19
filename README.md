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
    auth:
      auth-type: oauth2 | apikey
      # if oauth2:
      client-id: "your-client-id"                   # client id for oauth2
      client-secret: "your-client-secret"           # client secret for oauth2
      token-url: "https://example-1.com/token"      # token URL for oauth2
      # if apikey:
      api-key: "your-api-key"                       # API key for AASX Server
  - name: "AASX Server 2"
    url: "https://example-2.com/aasx-server-2"
    auth:
      # see example 1 from above
      ...
```

As an alternative to the config file, the above configuration can also be passed as JSON via the environment variable `APP_CONFIG`.

### Start IRS (Information Receiving Service)

Use `docker-compose.yml` in `examples` directory to start the IRS. Adapt the config file `config.yml` as described above.

```bash
docker-compose up -d
```

### Stop IRS

To stop the IRS, use the following command:

```bash
docker-compose down
```

### Use IRS

After starting the IRS, the API documentation is available at:
```
http://<your-host>:8080/swagger-ui/index.html
```