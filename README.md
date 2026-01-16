
# Brook-2PL

This repository showcases the Brook-2PL protocol, a deadlock-free Two-Phase Locking (2PL) mechanism optimized for high-contention database workloads. Brook-2PL introduces:

1. **SLW-Graph** for ensuring deadlock-free execution.
2. **Partial transaction chopping** for early lock release, improving concurrency.


See [our SIGMOD26](https://dl.acm.org/doi/10.1145/3769767) paper for more details on this protocol. 
Please cite our paper if you used this code.
```
@article{habibi2025brook,
  title={Brook-2PL: Tolerating High Contention Workloads with A Deadlock-Free Two-Phase Locking Protocol},
  author={Habibi, Farzad and Fang, Juncheng and Lorido-Botran, Tania and Nawab, Faisal},
  journal={Proceedings of the ACM on Management of Data},
  volume={3},
  number={6},
  pages={1--27},
  year={2025},
  publisher={ACM New York, NY, USA}
}
```


## Project Structure
### Experiment Results
[Experiment Results](https://github.com/gsoosk/Brook-2PL/tree/main/experiment-results) directory contains all the paper experiment results and their plots. 


### RetryFreeDB

[RetryFreeDB](https://github.com/gsoosk/Brook-2PL/tree/main/src/main/java/edgelab/retryFreeDB) implements a transaction data system featuring three concurrency control protocols:

- **Brook-2PL** (Deadlock-Free)
- **Wound-Wait**
- **Bamboo**

Key directories include:

- [`repo`](https://github.com/gsoosk/Brook-2PL/tree/main/src/main/java/edgelab/retryFreeDB/repo): Transaction system implementation.
- [`slwGraph`](https://github.com/gsoosk/Brook-2PL/tree/main/src/main/java/edgelab/retryFreeDB/slwGraph): Brook-2PL static analysis for deadlock-free transactions. `TXCheck.java` application in this repository can preprocess transactions into deadlock-free transactions. 
- [`benchmark`](https://github.com/gsoosk/Brook-2PL/tree/main/src/main/java/edgelab/retryFreeDB/benchmark): Benchmarking application for evaluating transaction system performance.

See the full list of supported operations by RetryFreeDB in the [RPC definitions](https://github.com/gsoosk/Brook-2PL/blob/main/src/main/proto/RetryFreeDB.proto).

## Getting Started

### Requirements
- Java 11+
- Maven
- Ansible

### Installation
Clone and build the repository:

```bash
git clone https://github.com/gsoosk/Brook-2PL.git
cd Brook-2PL
mvn clean install
```

### Data Initialization

To initialize the database, use [this Ansible script](https://github.com/gsoosk/Brook-2PL/blob/main/ansible/rocksdb_init.yml) by specifying the workload as either `tpcc` or `store`:

```bash
ansible-playbook ansible/rocksdb_init.yml
```

### Running the Server

Launch the RetryFreeDB server for interactive execution of transactions:

```bash
java -jar ./target/Server.jar <server_port> <server_address> <postgres_port> <db_tables_comma_separated>
```

### Performance Benchmarking

Or you can run benchmarks to evaluate the concurrency protocols. The benchmarking applications run transactions in Stored-Procedure mode and do not require a different execution of the RetryFreeDB Server:

```bash
java -jar ./target/Client.jar --help
usage: producer-performance [-h] --address ADDRESS --port PORT [--result-file RESULT-FILE] [--metric-file METRIC-FILE] [--partition-id PARTITION-ID] [--batch-size BATCH-SIZE]             
                            [--payload-delimiter PAYLOAD-DELIMITER] --throughput THROUGHPUT [--interval INTERVAL] [--timeout TIMEOUT] [--dynamic-batch-size DYNAMICBATCHSIZE]
                            [--dynamic-batch-time DYNAMICBATCHTIME] [--dynamic-interval DYNAMICINTERVAL] [--dynamic-interval-time DYNAMICINTERVALTIME] [--exponential-load EXPONENTIALLOAD]
                            [--dynamic-memory MEMORYTRIGGER] [--dynamic-memory-time MEMORYTRIGGERTIME] --hot-players HOTPLAYERS --hot-listings HOTLISTING --hot-selection-prob HOTSELECTION
                            [--max-threads MAXTHREADS] [--max-retry MAXRETRY] [--max-items-threads MAXITEMSTHREADS] --read-item-number READITEMNUMBER [--operation-delay OPERATIONDELAY]
                            [--2pl-mode 2PLMODE] [--benchmark-mode BENCHMARK_MODE] [--num-of-warehouses WAREHOUSECOUNT] [--hot-warehouses HOTWAREHOUSE] [--transaction-mode TRANSACTIONMODE]
                            (--num-records NUM-RECORDS | --benchmark-time BENCHMARK-TIME)

This tool is used to verify the producer performance.

named arguments:
  -h, --help             show this help message and exit
  --address ADDRESS      Server address for interactive client / Postgres address for stored procedure / Rocksdb directory
  --port PORT            Server port for interactive client / Postgres port for stored procedure / -1 For rocksdb
  --result-file RESULT-FILE
                         a csv file containing the total result of benchmark
  --metric-file METRIC-FILE
                         a csv file containing the timeline result of benchmark
  --partition-id PARTITION-ID
                         Id of the partition that you want to put load on (default: )
  --batch-size BATCH-SIZE
                         batch size in bytes. This producer batches records in this size and send them to kv store
  --payload-delimiter PAYLOAD-DELIMITER
                         provides delimiter to be used when --payload-file is provided. Defaults to new line.  Note  that this parameter will be ignored if --payload-file is not provided. (default:
                         \n)
  --throughput THROUGHPUT
                         throttle maximum message throughput to *approximately* THROUGHPUT messages/sec. Set this to -1 to disable throttling.
  --interval INTERVAL    interval between each packet.  Set this -1 to send packets blocking
  --timeout TIMEOUT      timeout of each batch request. It is two times of interval by default
  --dynamic-batch-size DYNAMICBATCHSIZE
                         dynamic batch size until a specific time
  --dynamic-batch-time DYNAMICBATCHTIME
                         deadline for a dynamic batch size
  --dynamic-interval DYNAMICINTERVAL
                         dynamic interval until a specific time
  --dynamic-interval-time DYNAMICINTERVALTIME
                         deadline for a dynamic interval
  --exponential-load EXPONENTIALLOAD
                         requests follow an exponential random distribution with lambda=1000/interval
  --dynamic-memory MEMORYTRIGGER
                         trigger the memory after trigger time to this amount
  --dynamic-memory-time MEMORYTRIGGERTIME
                         time of memory trigger
  --hot-players HOTPLAYERS
                         path to hot players
  --hot-listings HOTLISTING
                         path to hot listings
  --hot-selection-prob HOTSELECTION
                         chance of a hot record being selected
  --max-threads MAXTHREADS
                         number of maximum threads for sending the load
  --max-retry MAXRETRY   Maximum number of times a request can be retried (default: -1)
  --max-items-threads MAXITEMSTHREADS
                         number of maximum threads for reading item
  --read-item-number READITEMNUMBER
                         number of items to read at the same time
  --operation-delay OPERATIONDELAY
                         the amount of time each operation will be delayed, mimicking the thinking time.
  --2pl-mode 2PLMODE     2pl algorithm used in server. ww: Wound-Wait, bamboo: Bamboo, slw: SLW-Graph (default: slw)
  --benchmark-mode BENCHMARK_MODE
                         type of the benchmark used. could be either "store" or "tpcc". (default: store)
  --num-of-warehouses WAREHOUSECOUNT
                         number of warehouses in tpcc benchmark. (default: 1)
  --hot-warehouses HOTWAREHOUSE
                         number of hot warehouse records in tpcc benchmark. (default: 0)
  --transaction-mode TRANSACTIONMODE
                         Transaction execution mode. could be either "interactive" or "storedProcedure" (default: interactive)

  either --num-records or --benchmark-time must be specified but not both.

  --num-records NUM-RECORDS
                         number of messages to produce
  --benchmark-time BENCHMARK-TIME
                         benchmark time in seconds
```

Example TPC-C Benchmark command:

```bash
java -jar ./target/Client.jar \
  --address ./rocksdb/checkpoint \
  --port -1 \
  --throughput -1 \
  --benchmark-time 60 \
  --hot-selection-prob 0 \
  --max-threads 64 \
  --max-retry 10 \
  --operation-delay 0 \
  --2pl-mode slw \
  --benchmark-mode tpcc \
  --num-of-warehouses 16 \
  --hot-warehouses 16 \
  --transaction-mode storedProcedure
```

### Ansible Deployment

Ansible scripts are provided to automate the deployment of the server and benchmarking tools on remote servers. Refer to the [deployment scripts](https://github.com/gsoosk/Brook-2PL/tree/main/ansible/roles/build/files) for details.

For further information on Ansible deployments, refer to the dedicated [README](./ansible/README.md).

