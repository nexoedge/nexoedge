Nexoedge Dockerfiles
==================

Quickstart
----------

Initialize the Environment
^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Download the Nexoedge Debian packages for Ubuntu 22.04 (e.g., nexoedge-1.0-amd64-proxy.deb) to this project home directory and rename them accordingly.

   - Proxy: nexoedge-amd64-proxy.deb
   - Agent: nexoedge-amd64-agent.deb
   - CIFS: nexoedge-cifs.tar.gz
   - Utils: nexoedge-amd64-utils.deb
   - Full: nexoedge-amd64-full.deb
   - Admin portal: nexoedge-admin-portal.tar.gz

#. Install Docker

   .. code-block:: bash
  
     $ sudo ./install_docker.sh


Test-Run A Single-Proxy-Single-Agent Local Cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Steps
+++++

#. Run the example script to create a cluster and test file operations

   .. code-block:: bash

      $ ./examples/cifs/start_local_cluster.sh

Details of This Example
+++++++++++++++++++++++

``examples/cifs/start_local_cluster.sh``: Creating a single-proxy and single-agent Nexoedge cluster which communicates via the host IP

- Make sure

  #. Firewall allows incoming connections to ports 57002-57004 and 59001-59002
  #. No other applications are using the ports

- Procedures:

  #. Build the images
  #. Start a Redis instance as metadata store 
  #. Start a Proxy container with ports mapped to host
  #. Start an Agent container with ports mapped to host
  #. Execute a status check in Proxy container
  #. Check CIFS operations using a small file: List, upload, download, delete, list 
  #. Execute a status check in Proxy container
  #. Clean up the Proxy and Agent containers

- Expected outcomes:

  #. Images built successfully
  #. Proxy container started successfully with its container id printed to console
  #. Agent container started successfully with its container id printed to console
  #. Status check showed Proxy is connecting to 1 Agent
  #. CIFS operation check report success for all operations on small file, and downloaded file is not corrupted
  #. Containers removed


Run A Single-Proxy-Dual-Agent Local Cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Steps
+++++

#. Run the script to create a cluster

   .. code-block:: bash

      $ cd ./examples/cifs/docker-compose
      $ ./cmd.sh start


Building Nexoedge Docker Containers
-----------------------------------

Preparation
^^^^^^^^^^^

#. Download the Nexoedge Debian packages for Ubuntu 22.04 (e.g., nexoedge-1.0-amd64-proxy.deb) to this project home directory and rename them accordingly.

   - Proxy: nexoedge-amd64-proxy.deb
   - Agent: nexoedge-amd64-agent.deb
   - CIFS: nexoedge-cifs.tar.gz
   - Utils: nexoedge-amd64-utils.deb
   - Full: nexoedge-amd64-full.deb
   - Admin portal: nexoedge-admin-portal.tar.gz

#. Install Docker

   .. code-block:: bash

      $ sudo ./install_docker.sh

Build
^^^^^

#. Build the images

   .. code-block:: bash

      $ ./build_images.sh


Nexoedge Configurations
-----------------------

Overview
^^^^^^^^

Nexoedge can be configured by setting the corresponding environment variables. The environment variables are named in the following format:

``NCLOUD_[PROXY|AGENT|GENERAL|STORAGECLASS]_<Section>_<Key>``

- ``[PROXY|AGENT|GENERAL|STORAGECLASS]``: Name of the configuration file in upper case
- ``<Section>``: Section name with the first character capitalized
- ``<Key>``: Key name with the first character capitalized

If the environment variable of a configuration parameter is not specified or empty, its default value is used.

Generate environment file for Nexoedge configurations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For Nexoedge Proxy and Agent, the list of environment variables can be generated using ``gen_config_env_file.py`` under ``tools``, e.g., to generate all variables for ``proxy.ini``, ``general.ini``, ``agent.ini``, and ``storage_class.ini`` into the environment file ``test_env_file``,

.. code-block:: bash

  $ python3 gen_config_env_file.py general.ini NCLOUD_GENERAL > test_env_file
  $ python3 gen_config_env_file.py proxy.ini NCLOUD_PROXY >> test_env_file
  $ python3 gen_config_env_file.py agent.ini NCLOUD_AGENT >> test_env_file
  $ python3 gen_config_env_file.py storage_class.ini NCLOUD_STORAGECLASS >> test_env_file


Running Individual Nexoedge Components
--------------------------------------

#. Identify your host IP address for the Docker network

.. code-block:: bash

   $ ip addr show docker0

Proxy
^^^^^

#. Run a Redis container as metadata store

.. code-block:: bash

  $ sudo docker run -d --name=<redis-container-name> -p 6379:6379 redis

#. Run the Nexoedge proxy container with the metadata store IP address set to the host IP address

.. code-block:: bash

  $ sudo docker run -d --name=<proxy-container-name> --env-file=<env-file> -e NCLOUD_PROXY_Metastore_Ip=<host IP> -p 57002:57002 -p 59001:59001 -p 59002:59002 nexoedge-proxy

Agent
^^^^^

#. Run the Nexoedge agent container with the proxy IP address set to the host IP address

.. code-block:: bash

  $ sudo docker run -d --name=<agent-container-name> --env-file=<env-file> -e NCLOUD_GENERAL_Proxy01_Ip=<host IP> -p 57003:57003 -p 57004:57004 nexoedge-agent

CIFS with Proxy
^^^^^^^^^^^^^^^

#. Start a proxy by following the setup procedure in the previous subsections

#. Start the CIFS container

.. code-block:: bash

  $ sudo docker run -d --name=<cifs-container-name> -p 445:445 nexoedge-cifs

#. Copy the example Samba configuration file to current directory 

.. code-block:: bash

   $ cp cifs/smb.conf .
   
#. Update the target Nexoedge proxy IP in the configuration file `ncloud:ip = <host-docker-ip>`, e.g., `ncloud:ip = 172.17.0.1` for the default Docker network on a Ubuntu 22.04 server.

#. Copy the configuration file to the CIFS container

.. code-block:: bash

  $ sudo docker cp smb.conf <cifs-container-name>:/usr/local/samba/etc/smb.conf

#. Restart the CIFS container

.. code-block:: bash

  $ sudo docker restart <cifs-container-name>

#. Create the user for CIFS.

.. code-block:: bash

  sudo docker exec <container-name> useradd -M <username> # create user on linux without home directory
  sudo docker exec <container-name> usermod -L <username> # disable system logon

#. Create and set the passwords for CIFS users. (Execute the following command for the user `<username>` and type the password twice)

.. code-block:: bash
  sudo docker exec -i <container-name> /usr/local/samba/bin/pdbedit -a <username> -t # create user and set the password for CIFS


Admin Portal
^^^^^^^^^^^^

#. Create a webdis container accroding to Nicolas's example (https://github.com/nicolasff/webdis#try-in-docker)

.. code-block:: bash

  $ sudo docker run --rm -d -p 7379:7379 --name webdis nicolas/webdis

#. Add admin password to webdis

.. code-block:: bash

  $ sudo docker exec webdis redis-cli HMSET admin username admin password P@ssw0rD

#. Start Admin Portal container and expose the portal to port 3000 (of localhost)

.. code-block:: bash

  $ sudo docker run -d -e WEBDIS_REDIRECT_ADDR=<host ip>:7379 -p 3000:80 --name admin-portal nexoedge-admin-portal

#. Access the portal at `http://<host ip>:3000`


Miscs
-----

Nexoedge operations
^^^^^^^^^^^^^^^^^^^

- Check Nexoedge cluster status

  .. code-block:: bash

    $ sudo docker exec <proxy-container-name> ncloud-reporter /usr/lib/ncloud/current

- Clean metadata in Nexoedge: 

  .. code-block:: bash

    $ sudo docker exec <proxy-container-name> redis-cli FLUSHDB
