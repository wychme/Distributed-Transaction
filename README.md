# Distributed-Transaction
## Instructions 

Before running the code, run

```
make
```

to build the project.

After running the code, run

```
make clean
```

to cleanup.

### Server

Run the server as follows:

```
./server config.txt
```

The argument config.txt is the configuration file.

### Client

The client read inputs from stdin and send them to the coordinator. Run the client as follows:

```
./client config.txt
```

The argument config.txt is the configuration file.

The client need to be restarted in the input file reaches EOF (e.g. using command ./client config.txt < in.txt).
