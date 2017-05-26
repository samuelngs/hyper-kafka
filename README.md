# hyper-kafka
Apache Kafka message broker for Hyper framework

## Get Started
```
m := kafka.New(
  kafka.Addrs("localhost:9092"),
)
h := hyper.New(
  hyper.Message(m),
)
```
