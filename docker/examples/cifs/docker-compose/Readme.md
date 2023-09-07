# Docker Compose Example

This example will start a single Nexoedge cluster which consists of

| Component(s) | Service name(s) |
|--------------|-----------------|
| A proxy                           | `proxy`  |
| Two agents                        | `storage-node-01`, `storage-node-02`  |
| A Samba server with Nexoedge VFS  | `cifs`  |
| A metadata store                  | `metastore` |
| A statistics store                | `statsdb` |
| A reporter for statistics collection   | `reporter` |
| A portal frontend                 | `portal-frontend` |
| A portal backend                  | `portal-backend` |

The cluster setup is defined in the file `nexoedge-cluster.yml` with the service names listed in the above.

The variables for cluster setup is defined in the file `.env`.
- `TAG`: tag of the images to use, e.g., release date or version.
- `NEXOEDGE_DATA_DIR`: Parent directory for Docker bind mounts.

The script `cmd.sh` is used to start and terminate the cluster. The available commands can be listed by running `./cmd.sh`.
