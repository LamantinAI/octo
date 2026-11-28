# Kubernetes connector

The connector discovers and reads Kubernetes resources, including custom
resources, using the process's Kubernetes identity. `Client::try_default`
supports in-cluster ServiceAccount credentials and local kubeconfig. Register
`octo_connector_kubernetes::factory()` with the runtime builder and add the
example `kubernetes.toml` to the connector configuration directory.

Commands target the connector's configured id and reply with a correlated
`<command>.result` envelope. Errors return `error` (message), `reason`, and `code` (HTTP status when provided by Kubernetes).

- `kubernetes.resources`: discover resource names, kinds, API versions, scopes
  and supported read operations. Optional `api_version` narrows discovery to
  one group and version, for example `apps/v1` or `v1`.
- Discovery results are cached per connector for 60 seconds. Pass `refresh: true` to resources to reload discovery.
- `kubernetes.list`: `resource` is the discovered plural, for example `pods`
  or `applications`. Optional fields: `api_version`, `namespace`,
  `label_selector`, `limit` (1–500, default 100), and `continue` (the previous
  result's `metadata.continue`).
- `kubernetes.get`: `resource`, `name`, optional `api_version` and `namespace`.
- `kubernetes.logs`: pod `name`, `namespace`, optional `container` and
  `tail_lines` (1–1000, default 200). Output is limited to 256 KiB.

For generic reads, specify `api_version` to avoid ambiguity between API groups
and avoid discovering every API group. When omitted, discovery chooses a stable
version per group and requires an explicit `api_version` if a plural occurs in
multiple groups. The existing `pods`, `services`, `events` and `deployments`
commands retain their original core `v1` or `apps/v1` defaults.

A manifest `namespace` supplies the default for namespaced resources. Without a
default, list searches all namespaces; `namespace = "*"` explicitly selects all
namespaces for list even with a default. Get requires a namespace for namespaced
resources. Cluster-scoped resources ignore the configured default and reject an explicit namespace. Logs require a namespace.

Use `compact: true` on lists for names, namespaces and common status fields. Pagination metadata is preserved. Serialized replies exceeding 256 KiB return `ResponseTooLarge`; retry the same page with a smaller limit or compact output.

Kubernetes RBAC determines which calls succeed; cluster-wide list requires
cluster-wide permission. **Generic reads include Secrets if RBAC allows them.**
Resources and logs may contain sensitive data, so grant permissions accordingly.
Discovery reports API capabilities, not the caller's authorization.

For a deployment, configure a dedicated ServiceAccount, mount its projected API
token, allow API-server egress, and bind read-only permissions in each desired
namespace, or cluster-wide permissions as needed. Generic reads require `get` or
`list` on the requested resources; logs require `get` on `pods/log`. No write or
watch permission is needed. Discovery endpoints must also be accessible.
