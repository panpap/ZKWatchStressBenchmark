ZKWatchStressBenchmark
======================

Test Zookeeper behaviour and average responce time based on the clients simultaneously watching for new events.


Big number of Threads causes IOException, remember to run ulimit -n
and change the limits at /etc/security/limits.conf OR run ulimit -n 94000

./bin/zkServer.sh start-foreground

Output of the BenchMark should be .~.~.~ or Connection to ZK server is lost!
