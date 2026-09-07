"""Best-effort creation of the lifecycle topic on startup.

Redpanda in dev-container mode auto-creates topics on first produce, but that
gives a single partition. Creating it explicitly with a handful of partitions
lets the Phase 4 worker consume in parallel; ordering still holds because every
event is keyed by order_id.
"""

from __future__ import annotations


def ensure_topic(
    bootstrap_servers: str,
    topic: str,
    *,
    partitions: int = 6,
    replication_factor: int = 1,
    timeout: float = 5.0,
) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    if topic in admin.list_topics(timeout=timeout).topics:
        return

    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)]
    )
    for fut in futures.values():
        try:
            fut.result(timeout=10)
        except Exception:
            # Already exists (race) or the broker rejected it -- auto-create on
            # first produce still covers correctness.
            pass
