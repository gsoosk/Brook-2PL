# Paxos Postgres Ansible Playbooks

This repository provides two playbooks: 
* `init.yml`: installs requirements on the inventory servers (Java, Postgres, Docker, ...)
* `deploy.yml`: deploys `server` and `benchmark` programs on the remote servers 
    * Runs RocksDb on each of the servers
* `metric.yml`: deploys `prometheus` server on the specified server
* `rocksdb_init.yml`: initializes the workloads on the server

## How to run a playbook using ansible
```bash
ansible-playbook -v -i <inventory_file> <playbook_file>
```
