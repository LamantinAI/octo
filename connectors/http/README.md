# Dynamic HTTP connector authentication

Each connector can use a static API-key/bearer header or OAuth2 with a refresh
token. OAuth2 reuses `octo-http-auth`: one token cache per connector, shared with
its HTTP connection pool. Credentials are resolved when a command arrives.

## OAuth2

```toml
[connector.auth]
type = "oauth2"
token_url = "https://oauth2.googleapis.com/token"
client_id = "YOUR_CLIENT_ID.apps.googleusercontent.com"
client_secret_env = "GOOGLE_CLIENT_SECRET"
refresh_token_env = "GOOGLE_REFRESH_TOKEN"
```

`client_secret_env` and `refresh_token_env` name environment variables; they are
not secret values or `${secret.NAME}` references. Supply their values through
the runtime environment or a secret manager, outside the repository.

The connector exchanges the refresh token using a form-encoded POST with
`grant_type=refresh_token`, `client_id`, `client_secret`, and `refresh_token`.
It sends the resulting access token as `Authorization: Bearer ...`, replacing
any static Authorization header. Tokens are cached until 60 seconds before
expiry; refreshing happens on the next command. Token exchange uses
`[connector.timeout] request_ms`, or 30 seconds if unset. The API request has
its own request timeout.

Missing credentials, token endpoint failures, and invalid token responses emit
a correlated `<connector-id>.event.error` with `http_status = null`; the API
request is skipped. Token response bodies and transport details are omitted
from auth errors because they may contain credentials. API status retries
continue to use `[connector.retry]`; token exchange failures are not retried
within the command. A later command can attempt token exchange again. An API
401 is surfaced according to the existing retry policy; it does not force a
refresh or automatically replay a command.

## Google Calendar example

The opt-in [example manifest](examples/google_calendar.toml) lists calendar
events. It is outside the default `config/connectors/` scan.

1. Enable the Google Calendar API in a Google Cloud project, configure the
   consent screen, and create an OAuth client with an authorized redirect URI.
2. Complete Google's authorization-code flow with
   `scope=https://www.googleapis.com/auth/calendar.events.readonly` and
   `access_type=offline`. Exchange the authorization code using the same client
   and redirect URI to obtain a refresh token. Initial consent and code
   exchange are performed outside this connector. See
   [Google's authorization and refresh guide](https://developers.google.com/identity/protocols/oauth2/web-server)
   and [Calendar scopes](https://developers.google.com/workspace/calendar/api/auth).
3. Put the client ID in a deployment copy of the manifest and supply
   `GOOGLE_CLIENT_SECRET` and `GOOGLE_REFRESH_TOKEN` in the runtime environment.
   Keep the deployment copy and credentials outside Git working trees.
4. Register `octo_connector_http::factory()` under `"http"`, then add the
   deployment manifest to the runtime config:

   ```toml
   [[connectors.explicit]]
   path = "/path/to/deployment/google_calendar.toml"
   ```

Send `google_calendar.cmd.list_events`, targeted at `google_calendar`, with
`{"calendar_id":"primary"}`. The correlated
`google_calendar.event.events_listed` payload contains Google's JSON response.
If it contains `nextPageToken`, send `google_calendar.cmd.list_events_page`
with `{"calendar_id":"primary","page_token":"RETURNED_PAGE_TOKEN"}` and
repeat until no token remains. See the
[events.list reference](https://developers.google.com/workspace/calendar/api/v3/reference/events/list).

## Existing static headers

Existing manifests keep their header-injection behavior:

```toml
[connector.auth]
type = "bearer"
header = "Authorization"
secret_var = "${secret.api_token}"
value_prefix = "Bearer "

[connector.secrets]
api_token = { env = "API_TOKEN" }
```

For an API key, use `type = "api_key"`, the API's header name, and omit the
prefix. Omitting `[connector.auth]` leaves requests unauthenticated.
