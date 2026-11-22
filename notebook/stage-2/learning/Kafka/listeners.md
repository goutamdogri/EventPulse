# Kafka listeners and advertised listeners (Stage 2 cluster setup)

*Written while building `infra/compose/docker-compose.kafka.yml` for S2-F1.*

## The problem

A Kafka client connects in two steps:

1. It connects to **any** bootstrap broker from its list and asks for metadata.
2. The metadata answer says "partition 3 is led by broker 2, which you reach at **host:port**". The client then opens a connection to exactly that address.

The address in step 2 is the broker's **advertised listener**, not the address the client used in step 1. If a broker advertises `kafka2:9092`, a client on the host gets that name back, can't resolve `kafka2`, and fails. You then end up debugging connectivity instead of Kafka. Bootstrap works, the first produce times out.

## What changed from the PRD sketch and why

The PRD sketch has one listener, `PLAINTEXT://:9092`, advertised as `PLAINTEXT://kafka1:9092`. That only works for clients **inside** the Docker network. Our ingest-api and consumer-sink run **on the host** (pinned with `taskset`), so one advertised address can't serve everyone.

Every broker now has three listeners:

| Listener | Bind | Advertised | Who uses it |
| --- | --- | --- | --- |
| `INTERNAL` | `:19092` | `kafkaN:19092` | broker-to-broker replication (`KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL`), CLI tools run with `docker exec` |
| `EXTERNAL` | `:9092` inside the container | `localhost:9092` / `localhost:9094` / `localhost:9096` | clients on the host |
| `CONTROLLER` | `:9093` | not advertised | KRaft quorum traffic only (`KAFKA_CONTROLLER_LISTENER_NAMES`) |

Things I had to get right:

- **Different host port per broker.** All three containers listen on 9092 internally, but the host can only publish 9092 once, so they're published as 9092, 9094 and 9096. Each broker must **advertise its own host port** (`localhost:9094` for broker 2), otherwise a host client would be sent to broker 1 when it asks for broker 2.
- **`LISTENER_SECURITY_PROTOCOL_MAP` must name every listener**, including `CONTROLLER`. Custom names like `INTERNAL`/`EXTERNAL` aren't known protocols, so the map says they're all `PLAINTEXT`.
- **`INTER_BROKER_LISTENER_NAME`** has to point at a listener whose advertised address the *other brokers* can reach. `localhost:9094` means "myself" inside a container, so replication must use `INTERNAL` (`kafkaN:19092`).
- **The controller listener is never advertised.** Clients never talk to it. In KRaft combined mode each node is both broker and controller, and `CONTROLLER_QUORUM_VOTERS` uses the container names on port 9093.
- **Same `CLUSTER_ID` on all three**, or each node formats its storage as a different cluster and the quorum never forms.
- **Healthcheck and `kafka-init` use `INTERNAL`** (`localhost:19092` / `kafkaN:19092`), because inside a container the `EXTERNAL` advertised address `localhost:9092` points back to the container itself, which is only right for broker 1.

## How to check

```bash
# From the host (EXTERNAL): the client must see all three brokers at localhost addresses.
docker exec ep-kafka1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka1:19092 | grep -E '^[a-z]'
# Leader and ISR of every partition (INTERNAL):
docker exec ep-kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --describe --topic events.raw
```

## Other cluster choices worth remembering

- `auto.create.topics.enable=false`: a typo in a topic name fails loudly instead of creating a 1-partition, RF-whatever topic.
- `unclean.leader.election.enable=false`: if all in-sync replicas are gone, the partition stays offline rather than electing a stale replica and losing acknowledged writes.
- `min.insync.replicas=2` with RF 3 and `acks=all`: one broker can be down and writes still succeed; two down and writes are refused (`NOT_ENOUGH_REPLICAS`) instead of being acknowledged with one copy.
