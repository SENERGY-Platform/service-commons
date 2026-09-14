
## Migrating from ExchangeUserToken to ExchangeUserTokenV2

`ExchangeUserToken` uses Keycloak's legacy impersonation-style token exchange: given a
`requested_subject` (a user id), it asks Keycloak for a token *as that user*, authenticated
as a confidential client in the `master` realm.

`ExchangeUserTokenV2` implements Keycloak's ["Standard token exchange"](https://www.keycloak.org/securing-apps/token-exchange#_standard-token-exchange)
(RFC 8693) instead. Rather than impersonating a user by id, it exchanges an existing
`subject_token` (a token the caller already holds) for a new token scoped to a different
`audience` (client). This is the flow to use when a service already has a caller's token and
wants a token usable against another client/service, rather than acting as an arbitrary user.

### Signature change

```go
// v1
func ExchangeUserToken(keycloakEndpoint, clientId, clientSecret, userId string) (token Token, expiration time.Duration, err error)

// v2
func ExchangeUserTokenV2(keycloakEndpoint, clientId, clientSecret, subjectToken, audience string) (token Token, expiration time.Duration, err error)
```

| v1 parameter | v2 parameter | notes |
|---|---|---|
| `userId` | `subjectToken` | v1 takes the target user's id and impersonates them. v2 takes an existing access token (`subject_token`) to exchange. |
| — | `audience` | v2 additionally requires the client id the exchanged token should be scoped to. |

### Realm

`ExchangeUserToken` always targets the `master` realm. `ExchangeUserTokenV2` targets the realm
in the package variable `KeycloakRealm` (defaults to `"master"`). Set it before calling
`ExchangeUserTokenV2` if the target realm differs:

```go
jwt.KeycloakRealm = "my-realm"
```

### Keycloak configuration

Standard token exchange must be enabled for the client in Keycloak (it is not on by default).
See the "Standard token exchange" section of the linked docs for the required client permissions
and fine-grained admin permissions setup.

### Example

```go
// v1: impersonate a user by id
token, exp, err := jwt.ExchangeUserToken(keycloakEndpoint, clientId, clientSecret, userId)

// v2: exchange a token you already have for one scoped to another client
token, exp, err := jwt.ExchangeUserTokenV2(keycloakEndpoint, clientId, clientSecret, subjectToken, audience)
```
