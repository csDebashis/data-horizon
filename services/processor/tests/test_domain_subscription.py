"""Canonical Spark subscription and lag measurements exclude auxiliary mirrors."""
import pytest

from data_horizon.config import Settings
from data_horizon.processor import broker_end_offsets, transit_stream

pytestmark = pytest.mark.feature("F004")


@pytest.mark.parametrize('configured,expected', [(None, 20000), ('7500', 7500)], ids=['default', 'override'])
def test_transit_reader_has_one_typed_topic_and_never_skips_lost_data(monkeypatch, configured, expected):
    if configured is None:
        monkeypatch.delenv("MAX_OFFSETS_PER_TRIGGER", raising=False)
    else:
        monkeypatch.setenv("MAX_OFFSETS_PER_TRIGGER", configured)
    settings = Settings(kafka_servers="private-broker:9092")

    class Reader:
        options = {}

        def format(self, name):
            assert name == "kafka"
            return self

        def option(self, name, value):
            self.options[name] = value
            return self

        def load(self):
            return self
    reader = Reader()
    spark = type("Spark", (), {"readStream": reader})()
    assert transit_stream(spark, settings) is reader
    assert reader.options == {"kafka.bootstrap.servers": "private-broker:9092",
                              "subscribe": settings.transit_topic, "startingOffsets": "earliest",
                              "maxOffsetsPerTrigger": expected, "failOnDataLoss": "true"}
    assert not any(topic in reader.options.values() for kind, topic in settings.event_topics.items()
                   if kind != "transit-events")


def test_broker_lag_only_measures_transit_partitions(monkeypatch):
    import kafka

    settings = Settings(kafka_servers="private-broker:9092")
    calls = []

    class Consumer:
        def __init__(self, **kwargs):
            assert kwargs["group_id"] is None and kwargs["enable_auto_commit"] is False

        def partitions_for_topic(self, topic):
            calls.append(topic)
            return {0, 1, 2}

        def end_offsets(self, partitions):
            assert {partition.topic for partition in partitions} == {settings.transit_topic}
            return {partition: 100 + partition.partition for partition in partitions}

        def close(self):
            calls.append("closed")
    monkeypatch.setattr(kafka, "KafkaConsumer", Consumer)
    assert broker_end_offsets(settings) == {settings.transit_topic + f":{index}": 100 + index for index in range(3)}
    assert calls == [settings.transit_topic, "closed"]
