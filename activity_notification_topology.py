"""Deployment-hook operations for the Activity domain-event durable."""

from __future__ import annotations

import asyncio
import os
import sys

from nats.aio.client import Client as NATS
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, ReplayPolicy
from nats.js.errors import NotFoundError

from event_consumer import (
    ACK_WAIT_SECONDS,
    ACTIVITY_NOTIFICATION_SUBJECTS,
    MAX_DELIVERIES,
    NOTIFICATION_DURABLE,
)

STREAM = "MYOTA_EVENTS"
LEGACY_DURABLES = (
    "activity-notifications-pull-v1",
    "activity-notifications",
)


def desired_config() -> ConsumerConfig:
    return ConsumerConfig(
        durable_name=NOTIFICATION_DURABLE,
        filter_subjects=list(ACTIVITY_NOTIFICATION_SUBJECTS),
        deliver_policy=DeliverPolicy.ALL,
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=ACK_WAIT_SECONDS,
        max_deliver=MAX_DELIVERIES,
        max_ack_pending=64,
        max_waiting=32,
        backoff=[60, 120, 300, 300, 300, 300, 300, 300],
        replay_policy=ReplayPolicy.INSTANT,
        num_replicas=0,
        mem_storage=False,
        headers_only=False,
    )


def validate_config(actual: ConsumerConfig) -> None:
    desired = desired_config()
    fields = (
        "filter_subjects",
        "deliver_policy",
        "ack_policy",
        "ack_wait",
        "max_deliver",
        "max_ack_pending",
        "max_waiting",
        "backoff",
        "replay_policy",
        "num_replicas",
        "mem_storage",
        "headers_only",
        "deliver_subject",
    )
    for name in fields:
        actual_value = getattr(actual, name, None)
        desired_value = getattr(desired, name, None)
        if name == "filter_subjects":
            actual_value = tuple(actual_value or ())
            desired_value = tuple(desired_value or ())
        elif name == "deliver_subject":
            actual_value = actual_value or None
            desired_value = desired_value or None
        elif name in {"deliver_policy", "ack_policy", "replay_policy"}:
            actual_value = getattr(actual_value, "value", actual_value)
            desired_value = getattr(desired_value, "value", desired_value)
        elif name in {"mem_storage", "headers_only"}:
            actual_value = bool(actual_value)
            desired_value = bool(desired_value)
        if actual_value != desired_value:
            raise RuntimeError(
                f"{STREAM}/{NOTIFICATION_DURABLE} configuration drift: {name}"
            )


async def provision(js) -> None:
    stream = await js.stream_info(STREAM)
    if "myota.events.>" not in set(stream.config.subjects or ()):
        raise RuntimeError(
            "MYOTA_EVENTS does not capture all notification subjects"
        )
    config = desired_config()
    try:
        info = await js.consumer_info(STREAM, NOTIFICATION_DURABLE)
    except NotFoundError:
        info = await js.add_consumer(STREAM, config=config)
    validate_config(info.config)
    print(f"validated {STREAM}/{NOTIFICATION_DURABLE}")


async def retire_legacy(js) -> None:
    # Never retire a broad durable until the registered successor is valid.
    successor = await js.consumer_info(STREAM, NOTIFICATION_DURABLE)
    validate_config(successor.config)
    for durable in LEGACY_DURABLES:
        try:
            info = await js.consumer_info(STREAM, durable)
        except NotFoundError:
            continue
        config = info.config
        filters = tuple(config.filter_subjects or ())
        filter_subject = config.filter_subject or ""
        if filter_subject != "myota.events.>" or filters:
            raise RuntimeError(
                f"refusing to delete {STREAM}/{durable}: it is not the known broad legacy durable"
            )
        if config.ack_policy != AckPolicy.EXPLICIT:
            raise RuntimeError(
                f"refusing to delete {STREAM}/{durable}: unexpected acknowledgement policy"
            )
        await js.delete_consumer(STREAM, durable)
        print(f"retired {STREAM}/{durable}")


async def main() -> None:
    action = os.environ.get("ACTIVITY_NOTIFICATION_TOPOLOGY_ACTION")
    if action not in {"provision", "retire"}:
        raise RuntimeError(
            "ACTIVITY_NOTIFICATION_TOPOLOGY_ACTION must be provision or retire"
        )
    nc = NATS()
    await nc.connect(os.environ.get("NATS_URL", "nats://nats:4222"))
    try:
        js = nc.jetstream()
        if action == "provision":
            await provision(js)
        else:
            await retire_legacy(js)
    finally:
        await nc.drain()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print(
            f"Activity notification topology {sys.argv[0]} failed: {exc}",
            file=sys.stderr,
        )
        raise
