"""Actual domain-topic routing and transit-only Spark processing on Synthetic Docker."""
from __future__ import annotations

import json
import os

import pytest

from test_docker_acceptance import Deployment

pytestmark = [
    pytest.mark.integration, pytest.mark.evidence("synthetic", "integration"),
    pytest.mark.skipif(os.environ.get("RUN_BASELINE_DOCKER") != "1",
                       reason="Requires the explicitly launched fresh Synthetic baseline"),
]

SCRIPT = r"""
from datetime import timedelta
from pathlib import Path
import json,time,uuid
from kafka import KafkaConsumer,TopicPartition
from data_horizon.config import Settings
from data_horizon.collector import producer_for,publish,publish_auxiliary_snapshot,drain_outbox
from data_horizon.contracts import TransitEvent,utcnow
from data_horizon.store import Store
from data_horizon.event_topics import reassemble_snapshot_events
settings=Settings.load()
assert settings.mode=='synthetic'
run=uuid.uuid4().hex
settings.data_dir=Path('/data/baseline-62')/run
producer=producer_for(settings)
consumer=KafkaConsumer(bootstrap_servers=settings.kafka_servers.split(','),group_id=None,
    enable_auto_commit=False,value_deserializer=lambda value:json.loads(value.decode()))
try:
    assignments=[]
    deadline=time.monotonic()+30
    while time.monotonic()<deadline:
        assignments=[TopicPartition(topic,p) for topic in settings.event_topics.values()
                     for p in (consumer.partitions_for_topic(topic) or [])]
        if len({p.topic for p in assignments})==5:break
        time.sleep(.5)
    assert len({p.topic for p in assignments})==5
    consumer.assign(assignments)
    for partition,offset in consumer.end_offsets(assignments).items():consumer.seek(partition,offset)
    now=utcnow()
    transit=TransitEvent(source='synthetic',route_id='1',direction_id='0',trip_id='domain-'+run,
        stop_id='101N',event_time=now,predicted_time=now+timedelta(seconds=120),
        scheduled_time=now+timedelta(seconds=60),time_basis='predicted',match_confidence='exact').model_dump(mode='json')
    manifest=publish(settings,[transit],producer)
    expected={settings.transit_topic:{transit['event_id']}}
    snapshots={}
    payloads={
        'service-alerts':{'alerts':[],'fixture':run},
        'vehicle-status':{'vehicles':[],'fixture':run},
        'weather-observations':{'temperature_c':25,'observed_at':now.isoformat(),'fixture':run},
        'static-snapshots':{'static_version':'synthetic-'+run,'route_count':6,'stop_count':1,'fixture':run},
    }
    for kind,payload in payloads.items():
        manifest=publish_auxiliary_snapshot(settings,kind,payload,source='synthetic',
            retrieved_at=now,source_time=now,producer=producer)
        records=[json.loads(line) for line in Path(manifest['path']).read_text().splitlines()]
        expected[settings.topic_for(kind)]={record['event_id'] for record in records}
        snapshots[settings.topic_for(kind)]=payload
    seen={topic:{} for topic in expected}
    deadline=time.monotonic()+30
    while time.monotonic()<deadline and any(set(seen[t])!=ids for t,ids in expected.items()):
        for records in consumer.poll(timeout_ms=1000,max_records=1000).values():
            for record in records:
                identity=record.value.get('event_id')
                if identity in expected.get(record.topic,set()):seen[record.topic][identity]=record.value
    assert all(set(seen[t])==ids for t,ids in expected.items()),{t:len(v) for t,v in seen.items()}
    for topic,payload in snapshots.items():
        assert reassemble_snapshot_events(list(seen[topic].values()))==payload
        assert all(record['mode']=='synthetic' for record in seen[topic].values())
    assert all(not any(key in record for key in ('password','access_token','user_id'))
               for records in seen.values() for record in records.values())
    repeated=drain_outbox(settings,producer)
    assert repeated['published_events']==0 and repeated['pending_batches']==0
    store=Store(ensure_schema=False)
    deadline=time.monotonic()+180
    consumed=False;streaming={}
    while time.monotonic()<deadline:
        with store.connect() as connection:
            consumed=bool(store.execute(connection,'SELECT event_id FROM transit_event WHERE namespace=? AND event_id=?',
                                      ('synthetic',transit['event_id'])).fetchone())
        streaming=store.get_metadata('streaming') or {}
        if consumed and streaming.get('end_offsets'):break
        time.sleep(2)
    assert consumed,'Actual Spark did not publish the transit event into PostgreSQL'
    assert streaming.get('end_offsets')
    assert all(key.startswith(settings.transit_topic+':') for key in streaming['end_offsets'])
    print('DH_ACCEPTANCE_JSON='+json.dumps({'topic_count':len(seen),
        'records_by_topic':{topic:len(rows) for topic,rows in seen.items()},
        'snapshot_reassembly':True,'empty_snapshot_preserved':True,'acknowledged_retry_empty':True,
        'transit_reached_postgresql':consumed,'spark_end_offset_topics':sorted(streaming['end_offsets']),
        'private_fields_absent':True}))
finally:
    consumer.close();producer.close()
"""


@pytest.mark.case("F004-TC09")
def test_actual_five_domains_and_transit_only_spark(record_testsuite_property):
    target = Deployment()
    status = target.api("/api/v1/pipeline/status")
    assert status.status_code == 200 and status.json()["meta"]["mode"] == "synthetic"
    result = target.json_output(target.application_script(SCRIPT, timeout=240))
    assert result["topic_count"] == 5 and all(result["records_by_topic"].values())
    assert result["snapshot_reassembly"] and result["empty_snapshot_preserved"]
    assert result["acknowledged_retry_empty"] and result["transit_reached_postgresql"]
    record_testsuite_property("domain_topic_evidence", json.dumps(result, sort_keys=True))
