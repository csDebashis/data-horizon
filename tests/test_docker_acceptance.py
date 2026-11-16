"""Opt-in acceptance against the actual synthetic Docker deployment.

RUN_DOCKER=1 enables read-only checks and isolated spatial/analytics jobs. Service interruption,
container recreation and horizon_test restore additionally require
RUN_DOCKER_RECOVERY=1 during an agreed maintenance window. Nothing runs at import.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import time
import uuid

import httpx
import pytest

from scripts.retain_release import COMPONENTS, IMAGE_ENV, REVISION_LABEL

ROOT = Path(__file__).resolve().parents[1]
DOCKER = Path("/Applications/Docker.app/Contents/Resources/bin/docker")
PREFIX = "DH_ACCEPTANCE_JSON="
pytestmark = pytest.mark.integration


class Deployment:
    def __init__(self):
        self.binary = Path(os.environ.get("DOCKER", str(DOCKER))).resolve()
        self.environment = {**os.environ, "DATA_MODE": "synthetic"}
        self.environment["PATH"] = str(self.binary.parent) + os.pathsep + os.environ.get("PATH", "")
        self.base_url = os.environ.get("DOCKER_API_URL", "http://127.0.0.1:8000")
        self.token = (ROOT / ".secrets/api_token").read_text().strip()

    def safe(self, value):
        value = value.replace(self.token, "[redacted]") if self.token else value
        value = re.sub(r"(postgres(?:ql)?://[^:\s]+:)[^@\s]+@", r"\1[redacted]@", value)
        return re.sub(r"(?i)(Bearer\s+)\S+", r"\1[redacted]", value)

    def compose(self, *arguments, input=None, timeout=60, check=True, environment=None):
        result = subprocess.run(
            [str(self.binary), "compose", "--ansi", "never", *arguments],
            cwd=ROOT, env={**self.environment, **(environment or {})},
            input=input, capture_output=True, text=True, timeout=timeout,
        )
        if check and result.returncode:
            pytest.fail(self.safe(
                f"Docker operation failed ({result.returncode}): "
                + result.stderr[-3000:] + result.stdout[-1000:]
            ))
        return result

    def json_output(self, result):
        lines = [line[len(PREFIX):] for line in result.stdout.splitlines() if line.startswith(PREFIX)]
        assert lines, self.safe("No structured acceptance result: " + result.stdout[-1500:])
        return json.loads(lines[-1])

    def psql(self, query, database="horizon_test"):
        assert database in {"horizon", "horizon_test"}
        return self.compose(
            "exec", "-T", "postgres", "psql", "-U", "horizon_admin", "-d", database,
            "-v", "ON_ERROR_STOP=1", "-At", "-c", query,
        ).stdout.strip()

    def image(self, service):
        containers = self.compose("ps", "--all", "-q", service).stdout.split()
        assert containers, f"No {service} container"
        # Maintenance jobs share a Compose service label. Inspect the canonical
        # service container rather than accidentally joining CLI one-off IDs.
        template = '{"image":{{json .Image}},"oneoff":{{json (index .Config.Labels "com.docker.compose.oneoff")}}}'
        result = subprocess.run(
            [str(self.binary), "inspect", "--format", template, *containers],
            env=self.environment, capture_output=True, text=True, check=True, timeout=20,
        )
        rows = [json.loads(line) for line in result.stdout.splitlines() if line]
        canonical = [row["image"] for row in rows if str(row["oneoff"]).lower() == "false"]
        assert len(canonical) == 1, f"Expected one canonical {service} container"
        return canonical[0]

    def component_images(self):
        return {kind: self.image("database-init" if kind == "ops" else kind) for kind in COMPONENTS}

    def api(self, path):
        headers = {"Authorization": "Bearer " + self.token}
        with httpx.Client(base_url=self.base_url, headers=headers, timeout=10) as client:
            return client.get(path)

    def wait_postgres(self, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = self.compose("exec", "-T", "postgres", "pg_isready", "-U", "horizon_admin",
                                  "-d", "horizon", timeout=10, check=False)
            if result.returncode == 0:
                return
            time.sleep(1)
        pytest.fail("PostgreSQL did not become ready within its recovery deadline")

    def wait_api(self, timeout=120, fresh=False):
        deadline = time.monotonic() + timeout
        last = {}
        while time.monotonic() < deadline:
            try:
                response = self.api("/api/v1/pipeline/status")
                if response.status_code == 200:
                    last = response.json()
                    counts = last.get("data", {}).get("counts", {})
                    if counts.get("events", 0) > 0 and counts.get("metrics", 0) > 0:
                        if not fresh or last.get("meta", {}).get("stale") is False:
                            return last
            except httpx.HTTPError:
                pass
            time.sleep(2)
        pytest.fail("Docker pipeline readiness/freshness deadline exceeded: " + self.safe(json.dumps(last)))

    def application_script(self, script, *, spark=False, timeout=300, environment=None, check=True,
                           test_database=False):
        service = "processor" if spark else "database-init"
        arguments = ["run", "--rm", "--no-deps", "-T", "--entrypoint", "python",
                     "-e", "APP_ENV=test", "-e", "DATA_MODE=synthetic",
                     "-e", "DATA_DIR=/tmp/acceptance", "-e", "AUTO_INITIALIZE_SCHEMA=false",
                     "-e", "MIN_FREE_GB=0"]
        if spark:
            arguments += [
                "-e", "SPARK_LOCAL_IP=127.0.0.1", "-e", "SPARK_DRIVER_HOST=127.0.0.1",
                "-e", "SPARK_MASTER=local[2]", "-e", "SPARK_DRIVER_MEMORY=1g",
                "-e", "SPARK_PARTITIONS=2",
                "-e", "PYSPARK_SUBMIT_ARGS=--driver-memory 1g pyspark-shell",
            ]
        if test_database:
            secret_path = "/run/secrets/test_database_url"
            if spark:
                config = json.loads(self.compose("config", "--format", "json").stdout)
                volume = config["volumes"]["migration-secrets"]["name"]
                assert volume.startswith(config["name"] + "_")
                arguments += ["--volume", volume + ":/run/test-secrets:ro"]
                secret_path = "/run/test-secrets/test_database_url"
            arguments += ["-e", "TEST_DATABASE_URL_FILE=" + secret_path]
        arguments += [service, "-"]
        return self.compose(*arguments, input=script, timeout=timeout, check=check, environment=environment)

    def wait_kafka(self, timeout=120):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = self.compose(
                "exec", "-T", "kafka", "/opt/kafka/bin/kafka-broker-api-versions.sh",
                "--bootstrap-server", "kafka:9092", timeout=15, check=False,
            )
            if result.returncode == 0:
                return
            time.sleep(2)
        pytest.fail("Kafka did not become ready within its recovery deadline")


@pytest.fixture(scope="module")
def deployment():
    if os.environ.get("RUN_DOCKER") != "1":
        pytest.skip("BLOCKED: RUN_DOCKER=1 and the launched synthetic Docker application are required")
    if not DOCKER.exists() and not os.environ.get("DOCKER"):
        pytest.skip("BLOCKED: Docker CLI is unavailable")
    if not (ROOT / ".secrets/api_token").exists():
        pytest.skip("BLOCKED: bootstrap-generated API token is unavailable")
    target = Deployment()
    version = subprocess.run(
        [str(target.binary), "version", "--format", "{{.Server.Version}}"],
        cwd=ROOT, env=target.environment, capture_output=True, text=True, timeout=20,
    )
    if version.returncode or not version.stdout.strip():
        pytest.skip("BLOCKED: Docker daemon is unavailable")
    return target


@pytest.fixture
def recovery_window(deployment):
    if os.environ.get("RUN_DOCKER_RECOVERY") != "1":
        pytest.skip("BLOCKED: service interruption requires RUN_DOCKER_RECOVERY=1 in a quiet window")
    if os.environ.get("DOCKER_BENCHMARK_ACTIVE") == "1":
        pytest.skip("BLOCKED: recovery checks cannot interrupt an active performance benchmark")
    return deployment


@pytest.mark.case("F004-TC01", "F012-TC01")
@pytest.mark.evidence("synthetic", "integration")
def test_actual_docker_kafka_spark_postgres_api_flow(deployment, record_testsuite_property):
    status = deployment.wait_api(timeout=180, fresh=True)
    assert status["meta"]["mode"] == "synthetic"
    assert status["data"]["engine"] == "postgresql"
    assert status["data"]["processing"]["query_id"].startswith("spark-")
    assert status["data"]["counts"]["events"] > 0
    assert status["data"]["counts"]["metrics"] > 0
    assert deployment.api("/health/ready").status_code == 200
    record_testsuite_property("docker_app_image", deployment.image("api"))
    record_testsuite_property("docker_spark_image", deployment.image("processor"))
    for kind, identity in deployment.component_images().items():
        record_testsuite_property("docker_" + kind + "_image", identity)
    record_testsuite_property("docker_pipeline_evidence", json.dumps(status["data"]["processing"], sort_keys=True))


@pytest.mark.case("F012-TC03")
@pytest.mark.evidence("synthetic", "security")
def test_actual_component_images_have_isolated_python_projects(deployment, record_testsuite_property):
    modules = ("api", "collector", "processor", "cli", "benchmark")
    retired = ("ml", "model_dataset", "model_provenance", "actual_arrivals", "model_tools", "model_readiness")
    expected = {"api": {"api"}, "collector": {"collector"}, "processor": {"processor"},
                "ops": set(modules)}
    image_ids = deployment.component_images()
    provenance = {}
    for kind, wanted in expected.items():
        service = "database-init" if kind == "ops" else kind
        script = (
            "import importlib.util,json,shutil; "
            f"modules={modules!r}; retired={retired!r}; "
            "found={name:importlib.util.find_spec('data_horizon.'+name) is not None for name in modules}; "
            "absent={name:importlib.util.find_spec('data_horizon.'+name) is None for name in retired}; "
            "print('DH_ACCEPTANCE_JSON='+json.dumps({'modules':found,'retired_absent':absent,"
            "'core':importlib.util.find_spec('data_horizon.store') is not None,'java':shutil.which('java') is not None}))"
        )
        result = deployment.compose("run", "--rm", "--no-deps", "-T", "--entrypoint", "python",
                                    service, "-c", script, timeout=30)
        actual = deployment.json_output(result)
        assert actual["core"] is True
        assert all(actual["retired_absent"].values()), "A retired model module remains installed"
        assert {name for name, available in actual["modules"].items() if available} == wanted
        assert actual["java"] is (kind == "processor")
    for kind in expected:
        identity = image_ids[kind]
        inspected = subprocess.run(
            [str(deployment.binary), "image", "inspect", "--format",
             '{"revision":{{json .Config.Labels}},"architecture":{{json .Architecture}}}', identity],
            env=deployment.environment, capture_output=True, text=True, check=True, timeout=20,
        )
        details = json.loads(inspected.stdout)
        revision = (details["revision"] or {}).get(REVISION_LABEL, "")
        assert re.fullmatch(r"[a-f0-9]{40}", revision)
        provenance[kind] = {"image": identity, "revision": revision, "architecture": details["architecture"]}
        record_testsuite_property("docker_" + kind + "_image", identity)
    record_testsuite_property("docker_component_provenance", json.dumps(provenance, sort_keys=True))


@pytest.mark.case("F012-TC03", "F012-TC04")
@pytest.mark.evidence("synthetic", "security")
def test_actual_dashboard_is_a_token_only_node_project(deployment, record_testsuite_property):
    result = deployment.compose("exec", "-T", "dashboard", "node", "-e", (
        "const fs=require('node:fs');console.log(JSON.stringify({version:process.version,uid:process.getuid(),"
        "python:fs.existsSync('/usr/local/bin/python'),publicKey:!!process.env.PUBLIC_API_TOKEN_FILE,"
        "serviceKey:!!process.env.API_TOKEN_FILE,database:!!process.env.DATABASE_URL_FILE}))"
    ), timeout=20)
    actual = json.loads(result.stdout)
    assert actual["version"].startswith("v24.") and actual["uid"] == 10001
    assert actual["python"] is False and actual["publicKey"] is True and actual["serviceKey"] is True and actual["database"] is False
    identity = deployment.image("dashboard")
    inspected = subprocess.run(
        [str(deployment.binary), "image", "inspect", "--format", "{{index .Config.Labels \"org.opencontainers.image.revision\"}}", identity],
        env=deployment.environment, capture_output=True, text=True, check=True, timeout=20,
    )
    assert re.fullmatch(r"[a-f0-9]{40}", inspected.stdout.strip())
    record_testsuite_property("docker_dashboard_image", identity)
    record_testsuite_property("docker_dashboard_source", inspected.stdout.strip())


@pytest.mark.case("F012-TC03")
@pytest.mark.evidence("synthetic", "security")
def test_actual_standalone_node_production_readiness_rejects_unsafe_configuration(
    deployment, record_testsuite_property,
):
    # These are explicit fixture keys, never credentials copied from a runtime file.
    # Each real standalone server binds an ephemeral container-loopback port only.
    script = r"""
const {spawn}=require('node:child_process');
const net=require('node:net');
const {setTimeout:pause}=require('node:timers/promises');
const fixtureKey='readiness-fixture-public-key-'+'x'.repeat(32);
async function availablePort(){
  const server=net.createServer();
  await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve);});
  const port=server.address().port;
  await new Promise((resolve,reject)=>server.close(error=>error?reject(error):resolve()));
  return port;
}
async function stop(child){
  if(!child.pid||child.exitCode!==null||child.signalCode!==null)return;
  await new Promise(resolve=>{
    const timer=setTimeout(()=>child.kill('SIGKILL'),3000);
    child.once('exit',()=>{clearTimeout(timer);resolve();});
    child.kill('SIGTERM');
  });
}
async function status(port,path){
  const response=await fetch('http://127.0.0.1:'+port+path,{signal:AbortSignal.timeout(1000)});
  if(response.body)await response.body.cancel();
  return response.status;
}
async function scenario(name,overrides){
  const port=await availablePort();
  const env={...process.env,NODE_ENV:'production',APP_ENV:'production',HOSTNAME:'127.0.0.1',PORT:String(port),
    API_BASE_URL:'http://api:8000',DASHBOARD_PUBLIC_URL:'https://dashboard.example.invalid',
    PUBLIC_API_BASE_URL:'https://dashboard.example.invalid',PUBLIC_API_TOKEN_FILE:'',
    PUBLIC_API_TOKEN:fixtureKey,API_TOKEN_FILE:'',API_TOKEN:fixtureKey+'service',...overrides};
  const child=spawn(process.execPath,['server.js'],{cwd:process.cwd(),env,stdio:'ignore'});
  let startupError=false;
  child.once('error',()=>{startupError=true;});
  try{
    const deadline=Date.now()+12000;
    let ready=null;
    while(Date.now()<deadline){
      if(startupError||child.exitCode!==null||child.signalCode!==null)throw new Error('Standalone child stopped before readiness');
      try{ready=await status(port,'/health/ready');break;}catch{await pause(100);}
    }
    if(ready===null)throw new Error('Standalone readiness deadline exceeded');
    return {scenario:name,ready_status:ready,live_status:await status(port,'/health/live')};
  }finally{await stop(child);}
}
async function main(){
  const cases=[];
  cases.push(await scenario('valid-production',{}));
  cases.push(await scenario('missing-service-key',{API_TOKEN:''}));
  cases.push(await scenario('weak-public-key',{PUBLIC_API_TOKEN:'weak-fixture-key'}));
  console.log('DH_ACCEPTANCE_JSON='+JSON.stringify({execution:'actual-standalone-node',cases}));
}
main().catch(error=>{console.error('Isolated readiness probe failed: '+error.message);process.exitCode=1;});
"""
    name = "horizon-node-readiness-" + uuid.uuid4().hex[:12]
    try:
        result = deployment.compose(
            "run", "--rm", "--name", name, "--no-deps", "-T", "--entrypoint", "node",
            "dashboard", "-", input=script, timeout=75,
        )
        proof = deployment.json_output(result)
    finally:
        # This invocation owns only this uniquely named one-off container.
        subprocess.run([str(deployment.binary), "rm", "--force", name], cwd=ROOT,
                       env=deployment.environment, capture_output=True, timeout=20, check=False)
    assert proof == {
        "execution": "actual-standalone-node",
        "cases": [
            {"scenario": "valid-production", "ready_status": 200, "live_status": 200},
            {"scenario": "missing-service-key", "ready_status": 503, "live_status": 200},
            {"scenario": "weak-public-key", "ready_status": 503, "live_status": 200},
        ],
    }
    record_testsuite_property("docker_node_production_config", json.dumps(proof, sort_keys=True))


@pytest.mark.case("F012-TC03", "F012-TC04")
@pytest.mark.evidence("synthetic", "security")
def test_real_database_roles_and_reader_least_privilege(deployment):
    actual = deployment.psql(
        "SELECT has_table_privilege('horizon_reader','public.transit_event','SELECT'),"
        "has_table_privilege('horizon_reader','public.transit_event','INSERT'),"
        "has_table_privilege('horizon_writer','public.transit_event','INSERT'),"
        "has_table_privilege('horizon_writer','public.transit_event','UPDATE'),"
        "has_schema_privilege('horizon_reader','public','CREATE')", database="horizon",
    )
    assert actual == "t|f|t|t|f"
    result = deployment.compose("exec", "-T", "api", "python", "-c", (
        "from data_horizon.store import Store;"
        "s=Store(ensure_schema=False);"
        "c=s.connect(); db=c.__enter__();"
        "print(s.execute(db,'SELECT current_user AS name').fetchone()['name']);"
        "c.__exit__(None,None,None)"
    ))
    assert result.stdout.strip() == "horizon_reader"


@pytest.mark.spark
@pytest.mark.case("F006-TC07", "F009-COL05")
@pytest.mark.evidence("synthetic", "integration")
def test_real_java17_docker_sedona_and_parquet_codecs(deployment, record_testsuite_property):
    script = """
from pathlib import Path
import json, platform, tempfile
import pyarrow as pa
import pyarrow.parquet as pq
from data_horizon.processor import create_spark, validate_spatial
with tempfile.TemporaryDirectory(prefix='acceptance-spatial-') as directory:
    folder = Path(directory)
    spark = create_spark('DockerAcceptanceSpatial')
    spark.sparkContext.setLogLevel('ERROR')
    try:
        zstd_input=folder/'zstd-fixture.parquet'
        pq.write_table(pa.table({'value':[1,2,3]}),zstd_input,compression='zstd')
        assert sorted(row['value'] for row in spark.read.parquet(str(zstd_input)).collect())==[1,2,3]
        snappy_output=folder/'snappy-fixture'
        spark.range(3).write.option('compression','snappy').parquet(str(snappy_output))
        assert sorted(pq.read_table(snappy_output).column('id').to_pylist())==[0,1,2]
        geometry = validate_spatial(spark,[{'stop_id':'A','stop_lon':-73.98,'stop_lat':40.75}])
        assert geometry[0]['stop_id']=='A' and geometry[0]['valid'] is True
        assert geometry[0]['geometry']=='POINT (-73.98 40.75)'
        print('DH_ACCEPTANCE_JSON='+json.dumps({
            'python':platform.python_version(),
            'java':spark.sparkContext._jvm.java.lang.System.getProperty('java.version'),
            'spark':spark.version,'geometry':geometry,
            'parquet_codecs':{'zstd_spark_read':'passed','snappy_spark_write':'passed'}
        }))
    finally:
        spark.stop()
"""
    result = deployment.application_script(script, spark=True, timeout=180)
    proof = deployment.json_output(result)
    assert proof["java"].startswith("17.")
    assert proof["python"].startswith("3.11.")
    assert proof["geometry"] == [{"stop_id": "A", "valid": True, "geometry": "POINT (-73.98 40.75)"}]
    assert proof["parquet_codecs"] == {'zstd_spark_read': 'passed', 'snappy_spark_write': 'passed'}
    record_testsuite_property("docker_sedona_spatial_evidence", json.dumps(proof, sort_keys=True))


@pytest.mark.spark
@pytest.mark.case("F005-TC09")
@pytest.mark.evidence("synthetic", "integration")
def test_real_java17_docker_alert_selector_scope_parity(deployment, record_testsuite_property):
    image = deployment.image("processor")
    script = """
from datetime import datetime, timedelta, timezone
from pathlib import Path
import hashlib, inspect, json, math, platform
from data_horizon.analytics import aggregate
from data_horizon.contracts import TransitEvent
from data_horizon.processor import aggregate_spark, create_spark

now = datetime(2026, 11, 14, 12, tzinfo=timezone.utc)
rows = []
for route in ('1', '2'):
    for stop in ('A', 'B'):
        for minutes in (5, 10):
            instant = now - timedelta(minutes=minutes)
            rows.append(TransitEvent(
                source='synthetic', route_id=route, stop_id=stop, direction_id='0',
                trip_id=f'trip-{route}-{stop}-{minutes}', event_time=instant,
                observed_time=instant, scheduled_time=instant, time_basis='observed',
                match_confidence='exact', scheduled_headway_seconds=300,
                service_date='2026-11-14', ingested_at=now,
            ).model_dump(mode='json'))
facts = {'1': {'agency_id': 'MTA', 'route_type': '1'},
         '2': {'agency_id': 'OTHER', 'route_type': '3'}}
scenarios = [
    ('native_and_or', [{'id': 'paired', 'severity': 1.0, 'informed_entities': [
        {'route_id': '1', 'stop_id': 'A'}, {'route_id': '2', 'stop_id': 'B'}]}],
        {'1|A': [1, 90], '1|B': [0, 100], '2|A': [0, 100], '2|B': [1, 90]}),
    ('trip_direction_static_and_unknown', [
        {'id': 'exact-trip', 'severity': 1.0, 'informed_entities': [
            {'route_id': '1', 'stop_id': 'A', 'direction_id': 0,
             'trip': {'trip_id': 'trip-1-A-5', 'start_date': '20261114', 'direction_id': 0}}]},
        {'id': 'agency', 'severity': 0.5, 'informed_entities': [
            {'agency_id': 'MTA', 'route_type': 1}]},
        {'id': 'wrong-trip', 'severity': 1.0, 'informed_entities': [
            {'route_id': '1', 'stop_id': 'B', 'trip': {'trip_id': 'absent'}}]},
        {'id': 'wrong-direction', 'severity': 1.0, 'informed_entities': [
            {'route_id': '2', 'direction_id': 1}]},
        {'id': 'unknown-restriction', 'severity': 1.0, 'informed_entities': [
            {'route_id': '2', 'unsupported_wire_restriction': True}]},
        {'id': 'no-parent-guess', 'severity': 1.0, 'informed_entities': [{'stop_id': 'AN'}]},
    ], {'1|A': [2, 90], '1|B': [1, 95], '2|A': [0, 100], '2|B': [0, 100]}),
    ('legacy_conjunction', [{'id': 'legacy', 'severity': 1.0, 'route_ids': ['1'], 'stop_ids': ['B']}],
        {'1|A': [0, 100], '1|B': [1, 90], '2|A': [0, 100], '2|B': [0, 100]}),
]

def equivalent(actual, expected, path='root'):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys(), path
        for key in expected:
            equivalent(actual[key], expected[key], path + '.' + key)
    elif isinstance(expected, list):
        assert len(actual) == len(expected), path
        for index, (received, wanted) in enumerate(zip(actual, expected)):
            equivalent(received, wanted, path + '[' + str(index) + ']')
    elif isinstance(expected, float):
        assert math.isfinite(actual) and math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-9), path
    else:
        assert actual == expected, path

spark = create_spark('DockerAlertSelectorParity')
spark.sparkContext.setLogLevel('ERROR')
try:
    proof = {}
    for name, alerts, expected_entities in scenarios:
        reference = aggregate(rows, as_of=now, alerts=alerts, route_context=facts)
        actual = aggregate_spark(spark, rows, as_of=now, alerts=alerts, route_context=facts)
        equivalent(actual, reference)
        entities = {row['route_id'] + '|' + row['stop_id']: [row['alert_count'], row['health_score']]
                    for row in actual}
        assert entities == expected_entities, (name, entities, expected_entities)
        proof[name] = {'all_fields_equal': True, 'entities': entities}
    print('DH_ACCEPTANCE_JSON=' + json.dumps({
        'data_basis': 'synthetic', 'python': platform.python_version(), 'spark': spark.version,
        'java': spark.sparkContext._jvm.java.lang.System.getProperty('java.version'),
        'event_count': len(rows), 'scenarios': proof,
        'fixture_sha256': hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest(),
        'source_sha256': {name: hashlib.sha256(Path(inspect.getfile(function)).read_bytes()).hexdigest()
                          for name, function in [('reference', aggregate), ('processor', aggregate_spark)]},
    }, sort_keys=True))
finally:
    spark.stop()
"""
    result = deployment.application_script(script, spark=True, timeout=300, environment={"SPARK_IMAGE": image})
    proof = deployment.json_output(result)
    assert proof["java"].startswith("17.")
    assert proof["python"].startswith("3.11.")
    assert proof["event_count"] == 8 and len(proof["scenarios"]) == 3
    assert all(item["all_fields_equal"] is True for item in proof["scenarios"].values())
    inspected = subprocess.run(
        [str(deployment.binary), "image", "inspect", "--format",
         "{{index .Config.Labels \"org.opencontainers.image.revision\"}}", image],
        env=deployment.environment, capture_output=True, text=True, check=True, timeout=20,
    )
    revision = inspected.stdout.strip()
    assert re.fullmatch(r"[a-f0-9]{40}", revision)
    proof.update(processor_image=image, processor_revision=revision)
    record_testsuite_property("docker_spark_image", image)
    record_testsuite_property("docker_alert_selector_evidence", json.dumps(proof, sort_keys=True))


@pytest.mark.recovery
@pytest.mark.case("F012-TC06")
@pytest.mark.evidence("synthetic", "recovery")
def test_actual_encrypted_postgres_backup_restore_isolated_database(
    recovery_window, tmp_path, monkeypatch, record_testsuite_property,
):
    from scripts.backup import MAGIC, backup, restore

    target = recovery_window
    monkeypatch.chdir(ROOT)
    monkeypatch.setenv("DATA_MODE", "synthetic")
    monkeypatch.setenv("PATH", str(target.binary.parent) + os.pathsep + os.environ.get("PATH", ""))
    schema = "acceptance_" + uuid.uuid4().hex
    marker = uuid.uuid4().hex
    target.psql(f"CREATE SCHEMA {schema}; CREATE TABLE {schema}.fixture(id INT PRIMARY KEY,payload TEXT);"
                f"INSERT INTO {schema}.fixture VALUES(1,'{marker}'),(2,'restore-reference');")
    key = tmp_path / "independent-backup-key"
    key.write_text(secrets.token_hex(32))
    key.chmod(0o600)
    try:
        expected = target.psql(f"SELECT id,payload FROM {schema}.fixture ORDER BY id")
        output = tmp_path / "horizon-test.dump.aesgcm"
        result = backup(output, key, database="horizon_test")
        ciphertext = output.read_bytes()
        assert ciphertext.startswith(MAGIC) and marker.encode() not in ciphertext
        target.psql(f"DROP SCHEMA {schema} CASCADE")
        recovered = restore(output, key, "horizon_test", "horizon_test")
        actual = target.psql(f"SELECT id,payload FROM {schema}.fixture ORDER BY id")
        assert actual == expected
        assert result["off_host_copy_verified"] is False
        record_testsuite_property("isolated_postgres_restore", json.dumps({
            **result, **recovered, "canonical_sha256": hashlib.sha256(actual.encode()).hexdigest(),
            "limitation": "Logical restore on the same Docker host; no production WAL RPO or off-host RTO claim",
        }, sort_keys=True))
    finally:
        target.psql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.mark.recovery
@pytest.mark.case("F012-TC07")
@pytest.mark.evidence("synthetic", "recovery")
def test_rollback_requires_a_distinct_previously_verified_release(recovery_window, record_testsuite_property):
    import xml.etree.ElementTree as ET

    previous = os.environ.get("PRIOR_VERIFIED_RELEASE_MANIFEST")
    if not previous:
        pytest.skip("BLOCKED: no distinct previously verified five-component release manifest is supplied; reusing the same digest is not rollback")
    path = Path(previous).resolve()
    if not path.is_file() or path.stat().st_size > 1024 * 1024:
        pytest.skip("BLOCKED: the previous verified release manifest is unavailable or oversized")
    record = json.loads(path.read_text())
    if not all(record.get(field) is True for field in ("verified", "database_compatible", "checkpoint_compatible")):
        pytest.skip("BLOCKED: prior verification and database/checkpoint compatibility are required before rollback")
    if record.get("configuration_sha256") != hashlib.sha256((ROOT / "compose.yaml").read_bytes()).hexdigest():
        pytest.skip("BLOCKED: the supplied previous release does not certify this Compose configuration")
    evidence_path = (ROOT / record.get("regression_report", "")).resolve()
    if not evidence_path.is_relative_to((ROOT / "reports").resolve()) or not evidence_path.is_file():
        pytest.skip("BLOCKED: a previous Docker regression report under reports is required")
    report = ET.parse(evidence_path).getroot()
    if not list(report.iter("testcase")) or list(report.iter("failure")) or list(report.iter("error")):
        pytest.skip("BLOCKED: the previous Docker regression run is absent or failed")
    properties = {entry.get("name"): entry.get("value") for entry in report.findall(".//property")}
    target = recovery_window
    prior_images = record.get("component_images", {})
    if record.get("schema_version") != 2 or set(prior_images) != set(COMPONENTS):
        pytest.skip("BLOCKED: a qualified complete five-component manifest is required; historical image pairs are insufficient")
    if not all(re.fullmatch(r"sha256:[a-f0-9]{64}", value) for value in prior_images.values()):
        pytest.skip("BLOCKED: immutable inspected image IDs are required for all components")
    if any(properties.get("docker_" + kind + "_image") != image for kind, image in prior_images.items()):
        pytest.skip("BLOCKED: previous Docker evidence does not match every component image")
    if list(report.iter("skipped")) or record.get("regression_report_sha256") != hashlib.sha256(evidence_path.read_bytes()).hexdigest():
        pytest.skip("BLOCKED: complete passing checksum-linked previous evidence is required")
    current_images = target.component_images()
    if current_images == prior_images:
        pytest.skip("BLOCKED: the supplied set is the current release, so no rollback would occur")
    for image in prior_images.values():
        inspected = subprocess.run([str(target.binary), "image", "inspect", "--format", "{{.Id}}", image],
                                   env=target.environment, capture_output=True, text=True, timeout=20)
        if inspected.returncode or inspected.stdout.strip() != image:
            pytest.skip("BLOCKED: a required previous component image is not retained locally")
    prior = {IMAGE_ENV[kind]: image for kind, image in prior_images.items()}
    current = {IMAGE_ENV[kind]: image for kind, image in current_images.items()}
    services = ("runtime-init", "database-init", "collector", "processor", "api", "dashboard")
    def switch(images, environment):
        for job in ("runtime-init", "database-init"):
            target.compose("up", "-d", "--no-deps", "--no-build", job,
                           environment=environment, timeout=90)
            target.compose("wait", job, environment=environment, timeout=120)
            container = target.compose("ps", "--all", "-q", job,
                                       environment=environment).stdout.strip()
            assert container, f"No container was retained to verify the {job} exit code"
            state = subprocess.run(
                [str(target.binary), "inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", container],
                env=target.environment, capture_output=True, text=True, check=True, timeout=20,
            )
            assert state.stdout.strip() == "exited 0", f"{job} must complete successfully before switching services"
        long_running = tuple(service for service in services if service not in {"runtime-init", "database-init"})
        target.compose("up", "-d", "--no-deps", "--no-build", "--wait", "--wait-timeout", "240",
                       *long_running, environment=environment, timeout=280)
        assert target.component_images() == images
        target.wait_api(timeout=180, fresh=True)
        ui_url = os.environ.get("DOCKER_DASHBOARD_URL", "http://127.0.0.1:8501")
        assert httpx.get(ui_url + "/health/ready", timeout=10).status_code == 200
    try:
        switch(prior_images, prior)
    finally:
        switch(current_images, current)
    record_testsuite_property("actual_release_rollback", json.dumps({
        "previous": prior_images, "restored_current": current_images, "verified_report": str(evidence_path),
        "database_and_checkpoint_compatible": True, "compatibility_note": record.get("compatibility_note"),
    }, sort_keys=True))


@pytest.mark.case("F012-TC09")
@pytest.mark.evidence("live", "deployment")
def test_actual_linux_vm_deployment_gate():
    pytest.skip("BLOCKED: the requested current target is a local Mac; an actual Linux VM, DNS/TLS and off-host backup destination are not supplied")


def _store_prefix(schema, mode="synthetic"):
    assert re.fullmatch(r"acceptance_[a-f0-9]{32}", schema)
    assert mode in {"synthetic", "recorded"}
    return """
import json, os
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from psycopg.conninfo import conninfo_to_dict
from data_horizon.store import Store
os.environ['DATA_MODE']=__MODE_VALUE__
base = Path(os.environ['TEST_DATABASE_URL_FILE']).read_text().strip()
assert conninfo_to_dict(base).get('dbname') == 'horizon_test', 'Refusing a non-test database'
parts = urlsplit(base)
query = dict(parse_qsl(parts.query))
query['options'] = '-csearch_path=' + SCHEMA
dsn = urlunsplit((parts.scheme,parts.netloc,parts.path,urlencode(query),parts.fragment))
store = Store(data_dir='/tmp/acceptance-store',database_url=dsn,ensure_schema=True)
""".replace("SCHEMA", repr(schema)).replace("__MODE_VALUE__", repr(mode))


@pytest.fixture
def isolated_docker_schema(recovery_window):
    schema = "acceptance_" + uuid.uuid4().hex
    recovery_window.psql(f"CREATE SCHEMA {schema} AUTHORIZATION horizon_migrator")
    try:
        yield recovery_window, schema
    finally:
        recovery_window.wait_postgres()
        recovery_window.psql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def _delete_topics(target, topics):
    assert all(re.fullmatch(r"dh-acceptance-[a-f0-9-]+", topic) for topic in topics)
    script = """
from kafka import KafkaAdminClient
admin = KafkaAdminClient(bootstrap_servers='kafka:9092',request_timeout_ms=15000)
try:
    existing=set(admin.list_topics())
    selected=[topic for topic in TOPICS if topic in existing]
    if selected:
        admin.delete_topics(selected)
finally:
    admin.close()
""".replace("TOPICS", repr(topics))
    target.application_script(script, timeout=60)


def _create_archived_topic(target, topic, folder, *, publish_count=3, total=3):
    script = """
from datetime import timedelta
from pathlib import Path
import json
from kafka import KafkaAdminClient,KafkaProducer
from kafka.admin import NewTopic
from data_horizon.collector import write_events
from data_horizon.config import Settings
from data_horizon.contracts import TransitEvent,utcnow
class TestSettings(Settings):
    @property
    def event_topics(self):
        return {**super().event_topics, "transit-events": TOPIC}
    def topic_for(self, event_type):
        return TOPIC if event_type == "transit-events" else super().topic_for(event_type)
settings = TestSettings(mode='synthetic',data_dir=Path(FOLDER),kafka_servers='kafka:9092')
admin = KafkaAdminClient(bootstrap_servers='kafka:9092',request_timeout_ms=15000)
admin.create_topics([NewTopic(TOPIC,num_partitions=1,replication_factor=1)])
now=utcnow()
events=[]
for i in range(TOTAL):
    arrival=now-timedelta(minutes=5*(TOTAL-i-1))
    events.append(TransitEvent(source='docker-acceptance',route_id='1',stop_id='101N',
        trip_id='acceptance-trip-'+str(i),event_time=now,observed_time=arrival,
        scheduled_time=arrival-timedelta(seconds=120),time_basis='observed',
        match_confidence='exact',scheduled_headway_seconds=300).model_dump(mode='json'))
manifest=write_events(settings,events)
producer=KafkaProducer(bootstrap_servers='kafka:9092',acks='all',
                      value_serializer=lambda value:json.dumps(value).encode())
try:
    for event in events[:PUBLISH_COUNT]:
        producer.send(TOPIC,value=event).get(timeout=20)
    producer.flush(timeout=20)
finally:
    producer.close();admin.close()
print('DH_ACCEPTANCE_JSON='+json.dumps({'manifest':manifest,'count':len(events),
                                      'event_ids':[e['event_id'] for e in events]}))
"""
    for name, value in (("TOPIC", topic), ("FOLDER", folder), ("TOTAL", total), ("PUBLISH_COUNT", publish_count)):
        script = script.replace(name, repr(value))
    return target.json_output(target.application_script(script))


def _spark_consume(target, schema, topic, checkpoint, query_id, *, crash=False, expect_loss=False,
                   mode="synthetic", as_of=None):
    script = _store_prefix(schema, mode=mode) + """
from data_horizon.processor import create_spark,aggregate_spark
spark=create_spark('IsolatedDockerRecovery')
spark.sparkContext.setLogLevel('ERROR')
def consume(frame,batch_id):
    events=[json.loads(row['value']) for row in frame.selectExpr('CAST(value AS STRING) AS value').collect()]
    result=store.apply_batch(events,query_id=QUERY_ID,batch_id=str(batch_id),as_of=AS_OF,
        aggregate_fn=lambda rows,**kwargs:aggregate_spark(spark,rows,**kwargs))
    if CRASH and events:
        # Commit has returned, but Spark has not yet marked the batch complete.
        os._exit(73)
source=(spark.readStream.format('kafka').option('kafka.bootstrap.servers','kafka:9092')
    .option('subscribe',TOPIC).option('startingOffsets','earliest')
    .option('failOnDataLoss','true').option('maxOffsetsPerTrigger','100').load())
try:
    query=(source.writeStream.foreachBatch(consume).option('checkpointLocation',CHECKPOINT)
        .trigger(availableNow=True).start())
    query.awaitTermination()
    with store.connect() as connection:
        batches=store.execute(connection,'SELECT COUNT(*) AS n FROM processed_batch WHERE namespace=?',
                              (store.mode,)).fetchone()['n']
    print('DH_ACCEPTANCE_JSON='+json.dumps({'events':len(store.events()),'batches':batches,
        'metrics':store.metrics(),'data_loss_detected':False}))
except Exception as exc:
    text=str(exc).lower()
    if EXPECT_LOSS and (('lost' in text and 'offset' in text) or 'offsetoutofrange' in text
                        or 'data loss' in text or 'data may have been lost' in text):
        print('DH_ACCEPTANCE_JSON='+json.dumps({'data_loss_detected':True,
            'error_type':type(exc).__name__}))
    else:
        raise
finally:
    spark.stop()
"""
    for name, value in (("QUERY_ID", query_id), ("TOPIC", topic), ("CHECKPOINT", checkpoint),
                        ("CRASH", crash), ("EXPECT_LOSS", expect_loss), ("AS_OF", as_of)):
        script = script.replace(name, repr(value))
    result = target.application_script(script, spark=True, test_database=True, timeout=300, check=not crash)
    if crash:
        assert result.returncode == 73, target.safe("Expected post-commit termination: " + result.stderr[-2000:])
        return {"terminated_after_commit": True}
    return target.json_output(result)


@pytest.mark.recovery
@pytest.mark.case("F004-TC05")
@pytest.mark.evidence("synthetic", "recovery")
def test_spark_kill_after_commit_replays_without_duplicate_output(isolated_docker_schema, record_testsuite_property):
    target, schema = isolated_docker_schema
    identifier = uuid.uuid4().hex
    topic = "dh-acceptance-" + identifier
    checkpoint = "/data/acceptance/" + identifier + "/checkpoint"
    _create_archived_topic(target, topic, "/data/acceptance/" + identifier)
    try:
        _spark_consume(target, schema, topic, checkpoint, schema, crash=True)
        assert target.psql(f"SELECT COUNT(*) FROM {schema}.transit_event") == "3"
        before = target.psql(f"SELECT item_key,payload FROM {schema}.analytics_result ORDER BY item_key")
        proof = _spark_consume(target, schema, topic, checkpoint, schema)
        after = target.psql(f"SELECT item_key,payload FROM {schema}.analytics_result ORDER BY item_key")
        assert before == after
        assert proof["events"] == 3 and proof["batches"] == 1
        record_testsuite_property("spark_post_commit_recovery", json.dumps({
            "database": "horizon_test", "isolated_schema": schema, "topic": topic,
            "event_count": proof["events"], "committed_batches": proof["batches"],
            "canonical_sha256": hashlib.sha256(after.encode()).hexdigest(),
            "failure_point": "process exit73 after database commit, before Spark checkpoint commit",
        }, sort_keys=True))
    finally:
        _delete_topics(target, [topic])


@pytest.mark.recovery
@pytest.mark.case("F004-TC07")
@pytest.mark.evidence("synthetic", "recovery")
def test_own_topic_retention_gap_fails_and_archived_events_recover(isolated_docker_schema):
    target, schema = isolated_docker_schema
    identifier = uuid.uuid4().hex
    topic = "dh-acceptance-" + identifier
    recovered_topic = "dh-acceptance-" + uuid.uuid4().hex
    folder = "/data/acceptance/" + identifier
    archive = _create_archived_topic(target, topic, folder, publish_count=3, total=6)
    try:
        initial = _spark_consume(target, schema, topic, folder + "/checkpoint", schema)
        assert initial["events"] == 3
        script = """
from pathlib import Path
import json
from kafka import KafkaAdminClient,KafkaProducer,TopicPartition
events=[json.loads(line) for line in Path(ARCHIVE).read_text().splitlines()]
producer=KafkaProducer(bootstrap_servers='kafka:9092',acks='all',
                      value_serializer=lambda value:json.dumps(value).encode())
admin=KafkaAdminClient(bootstrap_servers='kafka:9092',request_timeout_ms=15000)
try:
    for event in events[3:]:
        producer.send(TOPIC,value=event).get(timeout=20)
    producer.flush(timeout=20)
    admin.delete_records({TopicPartition(TOPIC,0):6})
finally:
    producer.close();admin.close()
print('DH_ACCEPTANCE_JSON='+json.dumps({'deleted_before_offset':6}))
""".replace("ARCHIVE", repr(archive["manifest"]["path"])).replace("TOPIC", repr(topic))
        target.application_script(script)
        gap = _spark_consume(target, schema, topic, folder + "/checkpoint", schema, expect_loss=True)
        assert gap["data_loss_detected"] is True
        restore = """
from pathlib import Path
import hashlib,json
from kafka import KafkaAdminClient,KafkaProducer
from kafka.admin import NewTopic
content=Path(ARCHIVE).read_bytes()
assert hashlib.sha256(content).hexdigest()==CHECKSUM
admin=KafkaAdminClient(bootstrap_servers='kafka:9092',request_timeout_ms=15000)
admin.create_topics([NewTopic(TOPIC,num_partitions=1,replication_factor=1)])
producer=KafkaProducer(bootstrap_servers='kafka:9092',acks='all',
                      value_serializer=lambda value:json.dumps(value).encode())
try:
    for line in content.splitlines():
        producer.send(TOPIC,value=json.loads(line)).get(timeout=20)
    producer.flush(timeout=20)
finally:
    producer.close();admin.close()
""".replace("ARCHIVE", repr(archive["manifest"]["path"])).replace(
            "CHECKSUM", repr(archive["manifest"]["sha256"])).replace("TOPIC", repr(recovered_topic))
        target.application_script(restore)
        recovered = _spark_consume(target, schema, recovered_topic, folder + "/recovered-checkpoint",
                                   schema + "-recovered")
        assert recovered["events"] == 6
        assert target.psql(f"SELECT COUNT(DISTINCT event_id) FROM {schema}.transit_event") == "6"
    finally:
        _delete_topics(target, [topic, recovered_topic])


@pytest.mark.recovery
@pytest.mark.case("F004-TC06")
@pytest.mark.evidence("synthetic", "recovery")
def test_actual_broker_outage_retains_and_drains_collector_outbox(recovery_window):
    target = recovery_window
    identifier = uuid.uuid4().hex
    topic = "dh-acceptance-" + identifier
    folder = "/data/acceptance/" + identifier
    target.compose("stop", "--timeout", "20", "kafka", timeout=45)
    try:
        queued = """
from datetime import timedelta
from pathlib import Path
import json
from kafka.errors import NoBrokersAvailable
from data_horizon.collector import publish,producer_for
from data_horizon.config import Settings
from data_horizon.contracts import TransitEvent,utcnow
class TestSettings(Settings):
    @property
    def event_topics(self):
        return {**super().event_topics, "transit-events": TOPIC}
    def topic_for(self, event_type):
        return TOPIC if event_type == "transit-events" else super().topic_for(event_type)
settings=TestSettings(mode='synthetic',data_dir=Path(FOLDER),kafka_servers='kafka:9092')
try:
    connected=producer_for(settings)
except NoBrokersAvailable:
    connected=None
assert connected is None,'Broker should be unavailable during this check'
now=utcnow()
event=TransitEvent(source='docker-outage-fixture',route_id='1',trip_id='outage-trip',stop_id='101N',
    event_time=now,observed_time=now,scheduled_time=now-timedelta(seconds=120),
    time_basis='observed',match_confidence='exact').model_dump(mode='json')
try:
    publish(settings,[event],producer=None)
except ConnectionError:
    pass
else:
    raise AssertionError('Unavailable Kafka must leave a visible failed publication')
pending=list((settings.data_dir/'outbox'/'synthetic'/'pending').glob('*.json'))
assert len(pending)==1
print('DH_ACCEPTANCE_JSON='+json.dumps({'pending_batches':len(pending),'event_id':event['event_id']}))
""".replace("TOPIC", repr(topic)).replace("FOLDER", repr(folder))
        pending = target.json_output(target.application_script(queued))
        assert pending["pending_batches"] == 1
    finally:
        target.compose("start", "kafka")
        target.wait_kafka()
    drain = """
from pathlib import Path
import json
from kafka import KafkaAdminClient,KafkaConsumer
from kafka.admin import NewTopic
from data_horizon.collector import drain_outbox,producer_for
from data_horizon.config import Settings
class TestSettings(Settings):
    @property
    def event_topics(self):
        return {**super().event_topics, "transit-events": TOPIC}
    def topic_for(self, event_type):
        return TOPIC if event_type == "transit-events" else super().topic_for(event_type)
settings=TestSettings(mode='synthetic',data_dir=Path(FOLDER),kafka_servers='kafka:9092')
admin=KafkaAdminClient(bootstrap_servers='kafka:9092',request_timeout_ms=15000)
admin.create_topics([NewTopic(TOPIC,num_partitions=1,replication_factor=1)])
producer=producer_for(settings)
consumer=KafkaConsumer(TOPIC,bootstrap_servers='kafka:9092',auto_offset_reset='earliest',
    group_id=TOPIC,consumer_timeout_ms=30000,enable_auto_commit=False,
    value_deserializer=lambda value:json.loads(value.decode()))
try:
    sent=drain_outbox(settings,producer)
    record=next(iter(consumer))
    assert record.value['event_id']==EXPECTED
    repeated=drain_outbox(settings,producer)
    assert sent['pending_batches']==0 and repeated['published_events']==0
    print('DH_ACCEPTANCE_JSON='+json.dumps({'published':sent,'repeated':repeated,'event_id':record.value['event_id']}))
finally:
    consumer.close();producer.close();admin.delete_topics([TOPIC]);admin.close()
""".replace("TOPIC", repr(topic)).replace("FOLDER", repr(folder)).replace("EXPECTED", repr(pending["event_id"]))
    result = target.json_output(target.application_script(drain))
    assert result["published"]["published_events"] == 1
    target.wait_api(timeout=180, fresh=True)


@pytest.mark.recovery
@pytest.mark.case("F012-TC02")
@pytest.mark.evidence("synthetic", "recovery")
def test_postgres_data_survives_actual_container_recreation(isolated_docker_schema):
    target, schema = isolated_docker_schema
    marker = uuid.uuid4().hex
    target.psql(f"CREATE TABLE {schema}.persistence(value TEXT);INSERT INTO {schema}.persistence VALUES('{marker}')")
    before = target.compose("ps", "-q", "postgres").stdout.strip()
    target.compose("up", "-d", "--no-deps", "--no-build", "--force-recreate", "postgres", timeout=120)
    target.wait_postgres()
    after = target.compose("ps", "-q", "postgres").stdout.strip()
    assert before != after
    assert target.psql(f"SELECT value FROM {schema}.persistence") == marker
    target.wait_api(timeout=180, fresh=True)


@pytest.mark.recovery
@pytest.mark.case("F012-TC05")
@pytest.mark.evidence("synthetic", "recovery")
def test_real_processor_outage_becomes_stale_then_recovers(recovery_window, record_testsuite_property):
    target = recovery_window
    before = target.wait_api(timeout=180, fresh=True)
    target.compose("stop", "--timeout", "20", "processor", timeout=45)
    started = time.monotonic()
    became_stale = None
    try:
        deadline = started + 165
        while time.monotonic() < deadline:
            response = target.api("/api/v1/pipeline/status")
            assert response.status_code == 200
            if response.json()["meta"]["stale"] is True:
                became_stale = time.monotonic() - started
                break
            time.sleep(3)
        assert became_stale is not None,"Stopped processing must become visibly stale"
    finally:
        target.compose("start", "processor")
    after = target.wait_api(timeout=180, fresh=True)
    assert after["data"]["processing"]["as_of"] != before["data"]["processing"]["as_of"]
    record_testsuite_property("actual_processor_outage", json.dumps({
        "seconds_until_stale": became_stale,
        "seconds_until_recovered": time.monotonic()-started,
        "before_as_of": before["data"]["processing"]["as_of"],
        "after_as_of": after["data"]["processing"]["as_of"],
        "scope": "local Docker service recovery, not host failure/high availability",
    },sort_keys=True))


@pytest.mark.spark
@pytest.mark.case("F005-TC07")
@pytest.mark.evidence("recorded", "integration")
def test_genuine_recorded_kafka_structured_stream_matches_batch(
    isolated_docker_schema, record_testsuite_property,
):
    if os.environ.get("RUN_RECORDED") != "1":
        pytest.skip("BLOCKED: RUN_RECORDED=1 and the frozen genuine dataset are required")
    from data_horizon.analytics import aggregate
    from data_horizon.collector import ScheduleMatcher, decode_feed
    from data_horizon.config import Settings
    from data_horizon.contracts import timestamp
    from test_recorded import MANIFEST, freeze_capture, input_path
    from services.processor.tests.test_spark import assert_equivalent

    target, schema = isolated_docker_schema
    capture = freeze_capture()
    events, _, _ = decode_feed(
        input_path(capture, "realtime").read_bytes(), Settings(mode="recorded"),
        matcher=ScheduleMatcher(input_path(capture, "schedule_lookup")),
        static_version=capture["static_version"],
    )
    # Deterministically stratify an actual captured input; no synthetic values
    # are introduced, and source event times and confidence remain unchanged.
    selected = []
    for route in capture["routes"]:
        candidates = sorted(
            (row for row in events if row["route_id"] == route and row.get("predicted_time")
             and not row.get("cancelled")),
            key=lambda row: (row["stop_id"], row["trip_id"]),
        )
        selected.extend(candidates[:16])
    assert 1 <= len(selected) <= 100
    as_of = max(timestamp(row["event_time"]) for row in selected).isoformat()
    identifier = uuid.uuid4().hex
    topic = "dh-acceptance-" + identifier
    folder = "/data/acceptance/" + identifier
    script = """
from pathlib import Path
import json
from kafka import KafkaAdminClient,KafkaProducer
from kafka.admin import NewTopic
from data_horizon.collector import write_events
from data_horizon.config import Settings
events=json.loads(__ROWS__)
class TestSettings(Settings):
    @property
    def event_topics(self):
        return {**super().event_topics, "transit-events": __TOPIC__}
    def topic_for(self, event_type):
        return __TOPIC__ if event_type == "transit-events" else super().topic_for(event_type)
settings=TestSettings(mode='recorded',data_dir=Path(__FOLDER__),kafka_servers='kafka:9092')
manifest=write_events(settings,events)
admin=KafkaAdminClient(bootstrap_servers='kafka:9092',request_timeout_ms=15000)
admin.create_topics([NewTopic(__TOPIC__,num_partitions=1,replication_factor=1)])
producer=KafkaProducer(bootstrap_servers='kafka:9092',acks='all',
                      value_serializer=lambda value:json.dumps(value).encode())
try:
    for event in events:
        producer.send(__TOPIC__,value=event).get(timeout=20)
    producer.flush(timeout=20)
finally:
    producer.close();admin.close()
print('DH_ACCEPTANCE_JSON='+json.dumps({'archived_sha256':manifest['sha256'],'events':len(events)}))
""".replace("__ROWS__", repr(json.dumps(selected))).replace("__TOPIC__", repr(topic)).replace("__FOLDER__", repr(folder))
    archived = target.json_output(target.application_script(script))
    try:
        output = _spark_consume(
            target, schema, topic, folder + "/checkpoint", schema, mode="recorded", as_of=as_of,
        )
        expected = aggregate(selected, as_of=as_of)
        actual = sorted(output["metrics"], key=lambda row: (row["route_id"], row["direction_id"], row["stop_id"]))
        assert_equivalent(actual, expected)
        assert output["events"] == len({row["event_id"] for row in selected})
        record_testsuite_property("genuine_recorded_stream_parity", json.dumps({
            "dataset_manifest": str(MANIFEST), "selected_events": len(selected),
            "selected_archive_sha256": archived["archived_sha256"],
            "data_basis": "recorded", "database": "horizon_test", "namespace": "recorded",
            "engine": "actual Kafka to Spark Structured Streaming to PostgreSQL",
            "checkpoint": folder + "/checkpoint",
            "batch_metric_count": len(expected), "stream_metric_count": len(actual),
        }, sort_keys=True))
    finally:
        _delete_topics(target, [topic])



