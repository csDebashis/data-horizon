"""Evaluation evidence distinguishes real inputs from synthetic scenario labels."""
from __future__ import annotations

import hashlib
import json

import pytest

from data_horizon.graph import haversine_metres, what_if
import test_recorded
from test_recorded import input_path

# Register the shared fixture objects explicitly without importing a test module
# as a pytest plugin or adding global fixture behaviour to unrelated suites.
frozen_capture = test_recorded.frozen_capture


@pytest.mark.case("F013-TC04")
@pytest.mark.evidence("recorded", "integration")
def test_frozen_genuine_input_manifest_has_verifiable_checksums(frozen_capture):
    assert frozen_capture["data_basis"] == "recorded"
    assert frozen_capture["frozen_at_utc"] and frozen_capture["frozen_at_ist"]
    assert frozen_capture["source_quality_at_capture"]["accepted"] > 0
    assert {"static", "realtime", "schedule_lookup"}.issubset(
        {record["kind"] for record in frozen_capture["inputs"]}
    )
    for record in frozen_capture["inputs"]:
        path = input_path(frozen_capture, record["kind"])
        assert path.exists() and path.stat().st_size == record["size_bytes"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == record["sha256"]


def precision_recall_at_k(ordered_candidates, independently_labelled_nodes, k):
    if k < 1:
        raise ValueError("k must be positive")
    selected = set(ordered_candidates[:k])
    relevant = set(independently_labelled_nodes)
    true_positives = len(selected & relevant)
    return {
        "k": k,
        "precision": true_positives / len(selected) if selected else 0.0,
        "recall": true_positives / len(relevant) if relevant else None,
        "true_positives": true_positives,
    }


@pytest.mark.feature("F006", "F008", "F013")
@pytest.mark.evidence("synthetic", "evaluation")
def test_graph_and_distance_baseline_use_same_independent_synthetic_labels(record_testsuite_property):
    # Handwritten synthetic incident observations are supplied independently of
    # both algorithms. This deliberately small scenario is not real-world truth.
    observations = {"B": {"affected": True, "observed_after_minutes": 8},
                    "D": {"affected": True, "observed_after_minutes": 13},
                    "C": {"affected": False}}
    labels = {node for node, record in observations.items() if record["affected"]}
    candidates = {"B", "C", "D"}
    coordinates = {"A": (40.0, -74.0), "B": (40.01, -74.0),
                   "C": (40.0001, -74.0), "D": (40.02, -74.0)}
    edges = [
        {"source": "1|0|A", "target": "1|0|B", "travel_seconds": 300},
        {"source": "1|0|B", "target": "1|0|D", "travel_seconds": 300},
        {"source": "2|0|C", "target": "2|0|E", "travel_seconds": 300},
    ]
    graph = [row["stop_id"] for row in what_if(edges, "1", "A", 300, horizon_minutes=30)["affected_stations"]
             if row["stop_id"] in candidates]
    distance = sorted(candidates, key=lambda node: (
        haversine_metres(*coordinates["A"], *coordinates[node]), node,
    ))
    graph_metrics = precision_recall_at_k(graph, labels, 2)
    distance_metrics = precision_recall_at_k(distance, labels, 2)
    assert graph[:2] == ["B", "D"] and distance[:2] == ["C", "B"]
    assert graph_metrics["precision"] == graph_metrics["recall"] == 1.0
    assert distance_metrics["precision"] == distance_metrics["recall"] == 0.5
    results = {
        "data_basis": "synthetic", "candidate_count": len(candidates), "horizon_minutes": 30,
        "ground_truth": "independent handwritten synthetic incident observations",
        "graph": graph_metrics, "distance_baseline": distance_metrics,
        "limitation": "A toy scenario verifies the evaluation method; it does not establish real propagation accuracy",
    }
    record_testsuite_property("synthetic_graph_evaluation", json.dumps(results, sort_keys=True))


@pytest.mark.feature("F013")
def test_evaluation_does_not_invent_recall_when_no_nodes_are_labelled():
    result = precision_recall_at_k(["B"], set(), 1)
    assert result["precision"] == 0.0 and result["recall"] is None
    with pytest.raises(ValueError):
        precision_recall_at_k(["B"], {"B"}, 0)

