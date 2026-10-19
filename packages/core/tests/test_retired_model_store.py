"""Historical metadata stays inert without model-serving accessors or writes."""
import pytest

from data_horizon.store import Store

pytestmark = [pytest.mark.case('F009-COL05'), pytest.mark.feature('F009'),
              pytest.mark.evidence('synthetic', 'contract')]


def test_old_model_metadata_is_not_projected_or_served(isolated_data_dir):
    store = Store()
    store.put_metadata('model', {'status': 'available', 'artifact': '/private/historical-model'})
    store.put_metadata('last_training', {'status': 'completed', 'synthetic': True})
    assert {'model', 'model_status', 'experiment', 'predictions'}.isdisjoint(store.pipeline_status())
    assert not hasattr(store, 'model_status')
    assert not hasattr(store, 'predictions')
    assert store.get_metadata('model')['artifact'] == '/private/historical-model'


def test_retired_prediction_asset_writes_are_rejected_but_alerts_work(isolated_data_dir):
    store = Store()
    with pytest.raises(ValueError, match='Unsupported asset type'):
        store.replace_assets('predictions', [{'route_id': '1', 'prediction': 4}])
    store.replace_assets('alerts', [{'id': 'notice', 'route_ids': ['1']}])
    assert store.assets('alerts')[0]['id'] == 'notice'
