# Nexoedge Kubernetes Example

This example runs a Nexoedge cluster on a Kubernetes cluster. It requires `docker`, `kind` and `kubectl`.


## Architecture

Below shows the high-level architecture. It outlines the services and applications, as well as the communication between the applications, in the cluster.

```
+--- Namespace: nexoedge -----------------------------------------------------------------------------+
|               |                                                                                     |
|        svc:nexoedge-cifs                                                                            |
|               |                                                                                     |
|               v                                                                                     |
|     +--------------------+       +------------------------+                                         |
|     | app:nexoedge-cifs  |       | app:nexoedge-metastore |                                         |
|     +--------------------+       +------------------------+                                         |
|               |                               ^                                                     |
|               |                               |                                                     |
|               |                     svc:nexoedge-metastore                                          |
|               |                               |                                                     |
|               |                    +--------------------+                                           |
|    svc:nexoedge-internal-proxy --> | app:nexoedge-proxy | ---------------------+                    |
|             |  |                   +--------------------+                      |                    |
|             |  |                              |                                |                    |
|             |  |                  svc:nexoedge-internal-agent-1   svc:nexoedge-internal-agent-2     |
|             |  |                              |                                |                    |
|             |  |                              V                                V                    |
|             |  |                  +----------------------+          +----------------------+        |
|             |  |                  | app:nexoedge-agent-1 |          | app:nexoedge-agent-2 |        |
|             |  |                  +----------------------+          +----------------------+        |
|             |  |______________________________|                                |                    |
|             |__________________________________________________________________|                    |
|                                                                                                     |
+-----------------------------------------------------------------------------------------------------+
```

The 'app:nexoedge-cifs' application runs a CIFS service which exposes the CIFS storage access endpoint through the 'svc:nexoedge-cifs' service.

The 'app:nexoedge-proxy' application runs a Nexoedge proxy, which connects to the Redis in the 'app:nexoedge-metastore' application through the 'svc:nexoedge-metastore' service. It also receives storage requests from the 'app:nexoedge-cifs' application through the 'svc:nexoedge-internal-proxy' service.

Each of the 'app:nexoedge-agent-1' and 'app:nexoedge-agent-2' application runs a Nexoedge agent (with two storage containers), which registers to the Nexoedge proxy through the 'svc:nexoedge-internal-proxy' service. They expose the data access interfaces to the Nexoedge proxy through 'svc:nexoedge-agent-1' and 'svc:nexoedge-agent-2' services.


## Pre-requisites

This example starts a single-node Kind cluster and deploys Nexoedge components via the Kubernetes cluster management tool `kubectl`. Kind requires Docker to run.

It is tested on Ubuntu 22.04, but is expected to also work on other host platforms that support Docker, Kind, and `kubectl`.

To install `Docker`, refer to the [Docker official installation guide](https://docs.docker.com/engine/install/) or simply run the `install_docker.sh` script under "docker/" at the root of this repository.

To install `Kind` and `kubectl`, refer to the [Kind official installation guide](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) and [`kubectl` official installation guide](https://kubernetes.io/docs/tasks/tools/#kubectl).

The example requires port 445 of the host to be available for the CIFS service. If you want to host the service on another port, modify the value of 'hostPort' in `kind-cluster.yaml` to the port number to use. For file modify the value of the variable `port` in `upload-download-test.sh` to the port to use.


## How to run the example 

### Start the cluster

1. Build and run the Kind and Nexoedge cluster
   ```bash
   ./cmd.sh create
   ```

1. Wait until the all pods in the cluster are ready
   ```bash
   kubectl -n nexoedge get pods -w
   # press Ctrl+C to exit after all pods are in the "Running" state
   ```

### Run the Storage Service Test

1. Upload and download files to the default CIFS share
   ```bash
   ./upload-download-test.sh
   ```

### Stop the cluster

1. Stop the Nexoedge cluster (and leave the Kind cluster behind)
   ```bash
   ./cmd.sh term
   ```

### Start / Restart the cluster

1. Start the Nexoedge cluster after stopping (or restart the Nexoedge cluster)
   ```bash
   ./cmd.sh start
   ```

### Remove the cluster

1. Remove the Kind and Neoxedge cluster
   ```bash
   ./cmd.sh clean
   ```

### Other commands

1. Check by running the script without any arguments.
   ```bash
   ./cmd.sh
   ```


## Yaml File Organization

Nexoedge CIFS

- `nexoedge-cifs-deployment.yaml`: Deployment of the CIFS service as the `nexoedge-cifs` application
- `nexoedge-cifs-pv.yaml`: Persistent volumes for the CIFS service (file metadata)
- `nexoedge-cifs-pvc.yaml`: Persistent volume claims for the CIFS service (file metadata)
- `nexoedge-cifs-configmap.yaml`: Configurations for the CIFS service

Nexoedge Proxy

- `nexoedge-proxy-deployment.yaml`: Deployment of an Nexoedge proxy as the `nexoedge-proxy` application
- `nexoedge-proxy-configmap.yaml`: Configurations for the Nexoedge proxy


Nexoedge Agent

- `nexoedge-agent-1-deployment.yaml`: Deployment of an Nexoedge agent as the `nexoedge-agent-1` application
- `nexoedge-agent-2-deployment.yaml`: Deployment of an Nexoedge agent as the `nexoedge-agent-2` application
- `nexoedge-agents-pv.yaml`: Persistent volumes for the Nexoedge agents (storage containers)
- `nexoedge-agents-pvc.yaml`: Persistent volume claims for the Nexoedge agents (storage containers)
- `nexoedge-agents-configmap.yaml`: Configurations for the Nexoedge agents

Nexoedge Metadata Store

- `nexoedge-metastore-deployment.yaml`: Deployment of the Redis metadata store as the `nexoedge-metastore` application
- `nexoedge-metastore-pv.yaml`: Persistent volumes for the Redis (database)
- `nexoedge-metastore-pvc.yaml`: Persistent volume claims for the Redis (database)

General Nexoedge Cluster Setup

- `nexoedge-svc.yaml`: All services in the Nexoedge cluster
- `nexoedge-secret.yaml`: All the SSL keys for secure connections between the Nexoedge proxy and agents

Kind Cluster Setup

- `kind-cluster.yaml`: A basic Kind cluster configuration for setting up a single Kind cluster

