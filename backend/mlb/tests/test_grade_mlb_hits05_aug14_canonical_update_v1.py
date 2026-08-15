import pandas as pd

from backend.mlb.scripts import grade_mlb_hits05_aug14_canonical_update_v1 as g


def frame(probabilities,targets):
 return pd.DataFrame({'p_over':probabilities,'target':targets,'actual_hits':targets,'identity':[f'i{x}' for x in range(len(targets))]})


def test_metrics_use_fixed_probability_semantics():
 result=g.metrics(frame([.8,.2],[1,0]))
 assert result['resolved']==2 and abs(result['brier']-.04)<1e-12 and result['accuracy_at_50']==1


def test_ece_uses_fixed_bins():
 assert abs(g.ece(pd.Series([.51,.54]),pd.Series([1,0]))-.025)<1e-12


def test_effect_classification():
 before={'brier':.25,'log_loss':.70};improved={'brier':.24,'log_loss':.69};weakened={'brier':.26,'log_loss':.71}
 assert g.effect_classification(before,improved)=='AUG14_IMPROVED_CUMULATIVE_RECORD'
 assert g.effect_classification(before,weakened)=='AUG14_WEAKENED_CUMULATIVE_RECORD'
