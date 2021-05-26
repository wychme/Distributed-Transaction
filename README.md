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

The argument config.txt is the configuration file. It should have the format same as the config in https://courses.grainger.illinois.edu/cs425/sp2021/mps/mp3.html

Please run all the servers before running clients.

### Client

The client read inputs from stdin and send them to the coordinator. Run the client as follows:

```
./client config.txt
```

The argument config.txt is the configuration file. It should have the format same as the config in https://courses.grainger.illinois.edu/cs425/sp2021/mps/mp3.html

The client need to be restarted in the input file reaches EOF (e.g. using command ./client config.txt < in.txt).
