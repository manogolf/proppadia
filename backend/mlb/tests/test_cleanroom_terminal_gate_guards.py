import json
from pathlib import Path
import pytest
from backend.mlb.scripts.cleanroom_v1 import closeout_cleanroom_bol_tb15 as closeout
from backend.mlb.scripts.cleanroom_v1.lifecycle_guards import identity_certifiable, slate_signal_eligible
from backend.mlb.scripts.cleanroom_v1.package_existing_neutral_closeout import main as package_july30

def test_unsupported_missing_result_is_quarantined():
    assert not identity_certifiable('2026-07-29',823677,686469)

def test_historical_exception_cannot_enter_signal_evidence():
    assert not slate_signal_eligible('2026-07-29')
    assert not slate_signal_eligible('2026-07-30')
    assert not slate_signal_eligible('2026-07-31')
    assert slate_signal_eligible('2026-08-01')

def test_july31_exclusion_prevents_retrospective_freeze():
    with pytest.raises(SystemExit,match='INELIGIBLE_NEUTRAL_POPULATION'):
        closeout.freeze_neutral_population('2026-07-31')

def test_lifecycle_status_exposes_exception_and_ineligibility():
    status=closeout.lifecycle_status('2026-07-31')
    assert status['lifecycle_state']=='INELIGIBLE_FREEZE_MISSED'
    assert 'JULY31_NEUTRAL_POPULATION_NOT_FROZEN' in status['historical_exceptions_visible']

def test_july30_neutral_package_is_idempotent():
    assert package_july30()==0
    manifest=json.loads((closeout.EXPORT_ROOT/'2026-07-30/neutral_closeout/neutral_closeout_manifest.json').read_text())
    assert manifest['revision']==1 and manifest['membership_unchanged'] is True

def test_future_neutral_freeze_declares_outcome_independent_membership():
    source=Path(closeout.__file__).read_text()
    assert '"membership_uses_outcomes": False' in source
    assert '--freeze-only' in source

def test_closeout_requires_frozen_membership_and_missing_result_fails_closed():
    source=Path(closeout.__file__).read_text()
    assert 'closeout requires an existing immutable neutral population' in source
    assert 'outcome, settlement = "TECHNICAL_UNRESOLVED", "UNRESOLVED"' in source
    assert 'selected["player_mlb_id"] not in confirmed_starters' not in source

def test_signal_freezes_require_neutral_manifest():
    root=Path(closeout.ROOT)/'backend/mlb/scripts/cleanroom_v1'
    for name in ('manage_cleanroom_bol_tb15_under_hypotheses.py','manage_cleanroom_bol_tb15_under_toporder.py'):
        assert 'signal freeze requires neutral population freeze' in (root/name).read_text()
