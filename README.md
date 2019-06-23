
# kafka-connect [![Build Status](https://travis-ci.org/billryan/kafka-connect.svg?branch=master)](https://travis-ci.org/billryan/kafka-connect)

Extend `kafka-connect` according to source/sink protocol [kafka-connect-protocol](kafka-connect-protocol).

It contains connectors:

- [kafka-connect-protocol](kafka-connect-protocol) - protocol for source/sink connectors, data type and command type are defined in this module.
- [kafka-connect-redis](kafka-connect-redis) - sink to redis with [redisson/redisson](https://github.com/redisson/redisson).
- [kafka-connect-elasticsearch](kafka-connect-elasticsearch), thanks to [confluentinc/kafka-connect-elasticsearch](https://github.com/confluentinc/kafka-connect-elasticsearch).

## build

```
mvn clean package
```

### docker

Check [Dockerfile](Dockerfile) for detail, you can use [billryan/kafka-connect](https://hub.docker.com/r/billryan/kafka-connect) built by travis CI, change kafka broker address in [connect-distributed.properties](connect-distributed.properties).