A tool to backup Cassandra data to S3.

# Backup

# Restore
1. check backup info
    ```
    $ ./cassandrabackup list hosts \
        --cache-file=tmp.cache \
        --s3-region=<region-for-backup-files> \
        --s3-bucket=<bucket-for-backup-files> \
        --cluster=<cluster_name>

    // 2019-10-01T00:00:00.000Z        INFO    cassandrabackup/cassandrabackup.go:202  got_host        {"identity": {"cluster": "my_cluster", "hostname": "node-001"}}
    // ...
    // 2019-10-01T00:00:01.000Z        INFO    cassandrabackup/cassandrabackup.go:202  got_host        {"identity": {"cluster": "my_cluster", "hostname": "node-004"}}
    ```
1. set up a fresh Cassandra cluster with nodes listed in previous step (`node-001`...`node-004`)
1. stop all nodes (`sudo service cassandra stop`)
1. delete everything in `/var/lib/cassandra`
1. run `restore` on each node
    ```
    $ sudo ./cassandrabackup restore \
        --cache-file=tmp.cache \
        --s3-region=<region-for-backup-files> \
        --s3-bucket=<bucket-for-backup-files> \
        --cluster=<cluster_name> \
        --hostname=<host_name_for_current_node> \  # gotten by `list hosts`, e.g. `node-001`
    ```
1. start the cluster and check the status
    ```
    $ sudo service cassandra start
    $ nodetool status
    ```

# Troubleshoot
1. check the log if nodes do not start normally:
    ```
    $ sudo journalctl -f -u cassandra
    ```