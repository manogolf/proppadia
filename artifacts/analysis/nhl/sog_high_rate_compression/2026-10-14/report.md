# NHL SOG high-D10 compression

Classification: **SOFT_HIGH_RATE_COMPRESSION_SUPPORTED**. D10 feature classification: `CORE_CONTRIBUTOR_WITH_TWO_CORRECTABLE_BIASES`.

Control: `NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1`, replayed first using |D10−D20| > 0.5 midpoint, otherwise D10. The single fixed high-rate threshold is D10 > 9.0 SOG/60, consistent with prior historical top-decile cut 8.9270 and prospective cut 9.1235.

Soft form halves only corrected-rate excess above 9.0 for rows with D10 > 9.0. Hard cap sets corrected rate to 9.0 under the same activation condition. No threshold or weight search was performed.

Historical certified population: 13,389; retained D10/D20 evidence: 13,369; common valid replay rows: 13,347. Prospective October 10–13 settled feature-bound rows: 1,107.

## Replay validation

| population | comparison | n | mismatch_count | max_abs_difference | tolerance |
| --- | --- | --- | --- | --- | --- |
| historical | production | 13347 | 0 | 0.0000 | 0.0000 |
| historical | divergence_control | 13347 | 0 | 0.0000 | 0.0000 |
| prospective_oct10_13 | production | 1107 | 0 | 0.0000 | 0.0000 |
| prospective_oct10_13 | divergence_control | 1107 | 0 | 0.0000 | 0.0000 |

Corrected line probabilities are recomputed from the replayed lambda using the Poisson tail formula. Prospective aggregate Brier, log loss, calibration bias, predicted/realized rates, and accuracy match the frozen divergence artifact to floating point precision at all three lines. Historical aggregate line scores in that artifact use 13,369 rows, so they are not directly comparable to this study’s exact 13,347-row common intersection; the rowwise corrected-probability artifact is not retained. AUC and average-precision implementation differences are not used as probability replay checks.

| population | line | n_replayed | n_prior | population_matches | max_abs_metric_difference | comparison_status |
| --- | --- | --- | --- | --- | --- | --- |
| prospective | 1.5 | 1107 | 1107 | True | 0.0000 | EXACT_AGGREGATE_REPLAY |
| prospective | 2.5 | 1107 | 1107 | True | 0.0000 | EXACT_AGGREGATE_REPLAY |
| prospective | 3.5 | 1107 | 1107 | True | 0.0000 | EXACT_AGGREGATE_REPLAY |
| historical | 1.5 | 13347 | 13369 | False | 0.0119 | NONCOMPARABLE_POPULATION |
| historical | 2.5 | 13347 | 13369 | False | 0.0018 | NONCOMPARABLE_POPULATION |
| historical | 3.5 | 13347 | 13369 | False | 0.0021 | NONCOMPARABLE_POPULATION |

## Global count results

| population | group | variant | n | mae | rmse | mean_residual | median_absolute_error | mean_lambda | realized_mean_sog | brier_1.5 | log_loss_1.5 | auc_1.5 | average_precision_1.5 | calibration_bias_1.5 | predicted_over_rate_1.5 | realized_over_rate_1.5 | accuracy_1.5 | brier_2.5 | log_loss_2.5 | auc_2.5 | average_precision_2.5 | calibration_bias_2.5 | predicted_over_rate_2.5 | realized_over_rate_2.5 | accuracy_2.5 | brier_3.5 | log_loss_3.5 | auc_3.5 | average_precision_3.5 | calibration_bias_3.5 | predicted_over_rate_3.5 | realized_over_rate_3.5 | accuracy_3.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| historical | all | production | 13347 | 1.0354 | 1.3378 | -0.0110 | 0.9000 | 1.5382 | 1.5272 | 0.2184 | 0.6527 | 0.6970 | 0.6217 | 0.0014 | 0.4223 | 0.4209 | 0.6600 | 0.1492 | 0.4771 | 0.7299 | 0.4200 | 0.0044 | 0.2175 | 0.2131 | 0.7899 | 0.0817 | 0.2892 | 0.7578 | 0.2554 | 0.0048 | 0.1026 | 0.0978 | 0.8990 |
| historical | all | divergence_control | 13347 | 1.0211 | 1.3177 | -0.0141 | 0.8508 | 1.5414 | 1.5272 | 0.2145 | 0.6368 | 0.7052 | 0.6354 | 0.0044 | 0.4254 | 0.4209 | 0.6672 | 0.1469 | 0.4658 | 0.7380 | 0.4347 | 0.0037 | 0.2168 | 0.2131 | 0.7945 | 0.0802 | 0.2815 | 0.7688 | 0.2704 | 0.0028 | 0.1006 | 0.0978 | 0.9004 |
| historical | all | soft_compression | 13347 | 1.0169 | 1.3125 | 0.0019 | 0.8477 | 1.5253 | 1.5272 | 0.2141 | 0.6356 | 0.7056 | 0.6370 | 0.0024 | 0.4233 | 0.4209 | 0.6672 | 0.1463 | 0.4644 | 0.7384 | 0.4366 | 0.0005 | 0.2136 | 0.2131 | 0.7958 | 0.0796 | 0.2800 | 0.7695 | 0.2742 | -0.0006 | 0.0972 | 0.0978 | 0.9026 |
| historical | all | hard_cap | 13347 | 1.0154 | 1.3119 | 0.0180 | 0.8394 | 1.5093 | 1.5272 | 0.2139 | 0.6350 | 0.7058 | 0.6366 | -0.0003 | 0.4207 | 0.4209 | 0.6677 | 0.1462 | 0.4640 | 0.7384 | 0.4346 | -0.0031 | 0.2100 | 0.2131 | 0.7946 | 0.0795 | 0.2797 | 0.7695 | 0.2720 | -0.0040 | 0.0938 | 0.0978 | 0.9024 |
| prospective | all | production | 1107 | 1.0395 | 1.3475 | -0.0602 | 0.8000 | 1.5634 | 1.5032 | 0.2183 | 0.6296 | 0.6946 | 0.5925 | 0.0192 | 0.4321 | 0.4128 | 0.6540 | 0.1501 | 0.4725 | 0.7140 | 0.3744 | 0.0172 | 0.2214 | 0.2042 | 0.7877 | 0.0813 | 0.2936 | 0.7319 | 0.2381 | 0.0083 | 0.1032 | 0.0949 | 0.9015 |
| prospective | all | divergence_control | 1107 | 1.0286 | 1.3315 | -0.0596 | 0.8561 | 1.5628 | 1.5032 | 0.2156 | 0.6218 | 0.7004 | 0.6045 | 0.0213 | 0.4341 | 0.4128 | 0.6567 | 0.1478 | 0.4637 | 0.7207 | 0.3875 | 0.0159 | 0.2200 | 0.2042 | 0.7913 | 0.0801 | 0.2873 | 0.7372 | 0.2502 | 0.0060 | 0.1008 | 0.0949 | 0.8997 |
| prospective | all | soft_compression | 1107 | 1.0229 | 1.3234 | -0.0403 | 0.8529 | 1.5435 | 1.5032 | 0.2150 | 0.6192 | 0.7005 | 0.6110 | 0.0187 | 0.4315 | 0.4128 | 0.6576 | 0.1462 | 0.4603 | 0.7213 | 0.4007 | 0.0119 | 0.2160 | 0.2042 | 0.7940 | 0.0794 | 0.2859 | 0.7367 | 0.2673 | 0.0018 | 0.0967 | 0.0949 | 0.9051 |
| prospective | all | hard_cap | 1107 | 1.0179 | 1.3207 | -0.0211 | 0.8358 | 1.5243 | 1.5032 | 0.2144 | 0.6174 | 0.7007 | 0.6170 | 0.0154 | 0.4282 | 0.4128 | 0.6576 | 0.1451 | 0.4581 | 0.7224 | 0.4114 | 0.0074 | 0.2116 | 0.2042 | 0.8049 | 0.0794 | 0.2859 | 0.7346 | 0.2662 | -0.0023 | 0.0925 | 0.0949 | 0.9061 |

## High-rate results

| population | group | variant | n | mae | rmse | mean_residual | median_absolute_error | mean_lambda | realized_mean_sog | brier_1.5 | log_loss_1.5 | auc_1.5 | average_precision_1.5 | calibration_bias_1.5 | predicted_over_rate_1.5 | realized_over_rate_1.5 | accuracy_1.5 | brier_2.5 | log_loss_2.5 | auc_2.5 | average_precision_2.5 | calibration_bias_2.5 | predicted_over_rate_2.5 | realized_over_rate_2.5 | accuracy_2.5 | brier_3.5 | log_loss_3.5 | auc_3.5 | average_precision_3.5 | calibration_bias_3.5 | predicted_over_rate_3.5 | realized_over_rate_3.5 | accuracy_3.5 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| historical | d10_gt_9 | production | 1274 | 1.4294 | 1.7741 | -0.5409 | 1.3000 | 3.0448 | 2.5039 | 0.2084 | 0.6149 | 0.6666 | 0.7809 | 0.1011 | 0.7848 | 0.6837 | 0.6947 | 0.2430 | 0.6821 | 0.6543 | 0.5623 | 0.1191 | 0.5712 | 0.4521 | 0.5761 | 0.1879 | 0.5550 | 0.6732 | 0.3662 | 0.1153 | 0.3633 | 0.2480 | 0.7182 |
| historical | d10_gt_9 | divergence_control | 1274 | 1.3828 | 1.7287 | -0.4278 | 1.2000 | 2.9317 | 2.5039 | 0.2038 | 0.6020 | 0.6719 | 0.7876 | 0.0824 | 0.7660 | 0.6837 | 0.7025 | 0.2354 | 0.6646 | 0.6662 | 0.5746 | 0.0946 | 0.5467 | 0.4521 | 0.6020 | 0.1812 | 0.5387 | 0.6851 | 0.3778 | 0.0923 | 0.3404 | 0.2480 | 0.7292 |
| historical | d10_gt_9 | soft_compression | 1274 | 1.3391 | 1.6861 | -0.2597 | 1.1755 | 2.7637 | 2.5039 | 0.2001 | 0.5889 | 0.6771 | 0.7898 | 0.0605 | 0.7442 | 0.6837 | 0.7025 | 0.2296 | 0.6501 | 0.6673 | 0.5756 | 0.0611 | 0.5132 | 0.4521 | 0.6162 | 0.1748 | 0.5237 | 0.6874 | 0.3796 | 0.0568 | 0.3049 | 0.2480 | 0.7527 |
| historical | d10_gt_9 | hard_cap | 1274 | 1.3233 | 1.6811 | -0.0916 | 1.1485 | 2.5956 | 2.5039 | 0.1978 | 0.5830 | 0.6728 | 0.7836 | 0.0332 | 0.7168 | 0.6837 | 0.7080 | 0.2281 | 0.6464 | 0.6582 | 0.5612 | 0.0231 | 0.4752 | 0.4521 | 0.6028 | 0.1738 | 0.5205 | 0.6751 | 0.3602 | 0.0206 | 0.2686 | 0.2480 | 0.7512 |
| historical | top_decile | production | 1337 | 1.4180 | 1.7618 | -0.5444 | 1.3000 | 3.0268 | 2.4824 | 0.2100 | 0.6180 | 0.6688 | 0.7792 | 0.1029 | 0.7820 | 0.6791 | 0.6911 | 0.2413 | 0.6782 | 0.6623 | 0.5627 | 0.1216 | 0.5673 | 0.4458 | 0.5804 | 0.1852 | 0.5489 | 0.6800 | 0.3669 | 0.1157 | 0.3595 | 0.2438 | 0.7240 |
| historical | top_decile | divergence_control | 1337 | 1.3705 | 1.7144 | -0.4341 | 1.1998 | 2.9165 | 2.4824 | 0.2051 | 0.6044 | 0.6757 | 0.7872 | 0.0844 | 0.7635 | 0.6791 | 0.6993 | 0.2336 | 0.6604 | 0.6749 | 0.5768 | 0.0976 | 0.5434 | 0.4458 | 0.6058 | 0.1783 | 0.5320 | 0.6937 | 0.3813 | 0.0934 | 0.3372 | 0.2438 | 0.7360 |
| historical | top_decile | soft_compression | 1337 | 1.3289 | 1.6735 | -0.2739 | 1.1604 | 2.7563 | 2.4824 | 0.2015 | 0.5919 | 0.6800 | 0.7894 | 0.0635 | 0.7427 | 0.6791 | 0.6993 | 0.2280 | 0.6465 | 0.6755 | 0.5797 | 0.0656 | 0.5114 | 0.4458 | 0.6193 | 0.1721 | 0.5178 | 0.6954 | 0.3867 | 0.0596 | 0.3034 | 0.2438 | 0.7584 |
| historical | top_decile | hard_cap | 1337 | 1.3138 | 1.6687 | -0.1138 | 1.1384 | 2.5962 | 2.4824 | 0.1993 | 0.5862 | 0.6742 | 0.7837 | 0.0375 | 0.7166 | 0.6791 | 0.7046 | 0.2266 | 0.6431 | 0.6645 | 0.5683 | 0.0295 | 0.4752 | 0.4458 | 0.6066 | 0.1712 | 0.5147 | 0.6806 | 0.3727 | 0.0251 | 0.2689 | 0.2438 | 0.7569 |
| historical | top_5pct | production | 669 | 1.4947 | 1.8536 | -0.6791 | 1.3000 | 3.3203 | 2.6413 | 0.1970 | 0.5949 | 0.6689 | 0.8018 | 0.1096 | 0.8241 | 0.7145 | 0.7280 | 0.2499 | 0.6992 | 0.6600 | 0.5826 | 0.1500 | 0.6283 | 0.4783 | 0.5695 | 0.2076 | 0.6001 | 0.6656 | 0.3881 | 0.1476 | 0.4211 | 0.2735 | 0.6622 |
| historical | top_5pct | divergence_control | 669 | 1.4271 | 1.7897 | -0.5371 | 1.2000 | 3.1784 | 2.6413 | 0.1907 | 0.5764 | 0.6768 | 0.8075 | 0.0896 | 0.8041 | 0.7145 | 0.7354 | 0.2389 | 0.6742 | 0.6729 | 0.5916 | 0.1213 | 0.5996 | 0.4783 | 0.6099 | 0.1977 | 0.5771 | 0.6785 | 0.3961 | 0.1185 | 0.3920 | 0.2735 | 0.6801 |
| historical | top_5pct | soft_compression | 669 | 1.3508 | 1.7162 | -0.2616 | 1.1485 | 2.9029 | 2.6413 | 0.1848 | 0.5549 | 0.6890 | 0.8163 | 0.0549 | 0.7694 | 0.7145 | 0.7369 | 0.2289 | 0.6489 | 0.6755 | 0.6013 | 0.0674 | 0.5457 | 0.4783 | 0.6308 | 0.1864 | 0.5510 | 0.6829 | 0.4018 | 0.0606 | 0.3341 | 0.2735 | 0.7280 |
| historical | top_5pct | hard_cap | 669 | 1.3253 | 1.7090 | 0.0139 | 1.0888 | 2.6274 | 2.6413 | 0.1816 | 0.5464 | 0.6876 | 0.8236 | 0.0104 | 0.7249 | 0.7145 | 0.7474 | 0.2268 | 0.6431 | 0.6640 | 0.5982 | 0.0054 | 0.4838 | 0.4783 | 0.6069 | 0.1848 | 0.5461 | 0.6692 | 0.3895 | 0.0012 | 0.2747 | 0.2735 | 0.7265 |
| prospective | d10_gt_9 | production | 115 | 1.5041 | 1.8124 | -0.5526 | 1.4000 | 3.0221 | 2.4696 | 0.2161 | 0.6494 | 0.5870 | 0.7008 | 0.1027 | 0.7810 | 0.6783 | 0.7130 | 0.2687 | 0.7352 | 0.5595 | 0.4568 | 0.1489 | 0.5663 | 0.4174 | 0.4783 | 0.1954 | 0.5712 | 0.6500 | 0.3538 | 0.0979 | 0.3588 | 0.2609 | 0.7043 |
| prospective | d10_gt_9 | divergence_control | 115 | 1.4634 | 1.7610 | -0.4148 | 1.2832 | 2.8844 | 2.4696 | 0.2113 | 0.6285 | 0.6057 | 0.7196 | 0.0810 | 0.7592 | 0.6783 | 0.7217 | 0.2605 | 0.7148 | 0.5609 | 0.4659 | 0.1195 | 0.5369 | 0.4174 | 0.4957 | 0.1894 | 0.5573 | 0.6461 | 0.3681 | 0.0697 | 0.3305 | 0.2609 | 0.6870 |
| prospective | d10_gt_9 | soft_compression | 115 | 1.4085 | 1.7009 | -0.2295 | 1.2544 | 2.6991 | 2.4696 | 0.2054 | 0.6036 | 0.6171 | 0.7533 | 0.0560 | 0.7343 | 0.6783 | 0.7304 | 0.2459 | 0.6820 | 0.5737 | 0.5036 | 0.0814 | 0.4987 | 0.4174 | 0.5217 | 0.1826 | 0.5441 | 0.6498 | 0.4100 | 0.0298 | 0.2907 | 0.2609 | 0.7391 |
| prospective | d10_gt_9 | hard_cap | 115 | 1.3601 | 1.6807 | -0.0442 | 1.1356 | 2.5138 | 2.4696 | 0.1997 | 0.5867 | 0.6429 | 0.7925 | 0.0245 | 0.7028 | 0.6783 | 0.7304 | 0.2353 | 0.6601 | 0.5972 | 0.5257 | 0.0382 | 0.4556 | 0.4174 | 0.6261 | 0.1821 | 0.5442 | 0.6420 | 0.4049 | -0.0104 | 0.2505 | 0.2609 | 0.7478 |
| prospective | top_decile | production | 111 | 1.5223 | 1.8307 | -0.5635 | 1.4000 | 3.0410 | 2.4775 | 0.2184 | 0.6555 | 0.5839 | 0.6972 | 0.1081 | 0.7837 | 0.6757 | 0.7117 | 0.2712 | 0.7405 | 0.5559 | 0.4538 | 0.1560 | 0.5704 | 0.4144 | 0.4685 | 0.1978 | 0.5769 | 0.6400 | 0.3504 | 0.1017 | 0.3629 | 0.2613 | 0.7027 |
| prospective | top_decile | divergence_control | 111 | 1.4793 | 1.7777 | -0.4216 | 1.3000 | 2.8990 | 2.4775 | 0.2133 | 0.6337 | 0.6031 | 0.7166 | 0.0857 | 0.7614 | 0.6757 | 0.7207 | 0.2625 | 0.7189 | 0.5572 | 0.4626 | 0.1257 | 0.5401 | 0.4144 | 0.4865 | 0.1916 | 0.5627 | 0.6352 | 0.3645 | 0.0725 | 0.3338 | 0.2613 | 0.6847 |
| prospective | top_decile | soft_compression | 111 | 1.4223 | 1.7159 | -0.2298 | 1.2591 | 2.7073 | 2.4775 | 0.2071 | 0.6078 | 0.6130 | 0.7497 | 0.0599 | 0.7356 | 0.6757 | 0.7297 | 0.2474 | 0.6848 | 0.5669 | 0.4977 | 0.0863 | 0.5007 | 0.4144 | 0.5135 | 0.1845 | 0.5489 | 0.6333 | 0.4013 | 0.0313 | 0.2926 | 0.2613 | 0.7387 |
| prospective | top_decile | hard_cap | 111 | 1.3721 | 1.6951 | -0.0381 | 1.1825 | 2.5156 | 2.4775 | 0.2013 | 0.5903 | 0.6380 | 0.7878 | 0.0273 | 0.7030 | 0.6757 | 0.7297 | 0.2364 | 0.6622 | 0.5908 | 0.5168 | 0.0417 | 0.4561 | 0.4144 | 0.6216 | 0.1840 | 0.5489 | 0.6253 | 0.3905 | -0.0103 | 0.2510 | 0.2613 | 0.7477 |
| prospective | top_5pct | production | 56 | 1.8109 | 2.0944 | -0.6499 | 1.5500 | 3.4178 | 2.7679 | 0.2293 | 0.7084 | 0.3930 | 0.6513 | 0.1277 | 0.8420 | 0.7143 | 0.7143 | 0.3002 | 0.8086 | 0.4974 | 0.4479 | 0.2050 | 0.6514 | 0.4464 | 0.4464 | 0.2517 | 0.6976 | 0.4879 | 0.3412 | 0.1027 | 0.4419 | 0.3393 | 0.5714 |
| prospective | top_5pct | divergence_control | 56 | 1.7617 | 2.0223 | -0.4755 | 1.5926 | 3.2434 | 2.7679 | 0.2223 | 0.6745 | 0.4383 | 0.6734 | 0.1068 | 0.8211 | 0.7143 | 0.7321 | 0.2881 | 0.7766 | 0.5019 | 0.4661 | 0.1718 | 0.6183 | 0.4464 | 0.4464 | 0.2432 | 0.6810 | 0.4950 | 0.3675 | 0.0664 | 0.4057 | 0.3393 | 0.5714 |
| prospective | top_5pct | soft_compression | 56 | 1.6533 | 1.9189 | -0.1512 | 1.5668 | 2.9190 | 2.7679 | 0.2115 | 0.6252 | 0.4719 | 0.7031 | 0.0648 | 0.7791 | 0.7143 | 0.7321 | 0.2601 | 0.7135 | 0.5523 | 0.5225 | 0.1063 | 0.5527 | 0.4464 | 0.4643 | 0.2296 | 0.6553 | 0.5235 | 0.3918 | -0.0034 | 0.3359 | 0.3393 | 0.6429 |
| prospective | top_5pct | hard_cap | 56 | 1.5583 | 1.8831 | 0.1731 | 1.5507 | 2.5947 | 2.7679 | 0.2007 | 0.5914 | 0.5492 | 0.7614 | 0.0102 | 0.7244 | 0.7143 | 0.7321 | 0.2395 | 0.6716 | 0.6277 | 0.6016 | 0.0309 | 0.4773 | 0.4464 | 0.6607 | 0.2285 | 0.6553 | 0.5733 | 0.4148 | -0.0740 | 0.2653 | 0.3393 | 0.6607 |

Near-agreement high-rate rows are the independent-mechanism test; overlap rows split high D10 by divergence category. See `tail_metrics.csv` and `overlap_metrics.csv` for the complete outputs.

## Safety and stability

Activation counts, unchanged-row assertions, mid-range and low-rate safety, D10-decile residual curves, date-level comparisons, equal-player counts, TOI bands, prospective history depth, market common-row scores, and fixed-D20 diagnostics are in the named CSVs. The October 14 capture was not graded because no final immutable official-outcome reconciliation package was available. It is excluded from the frozen comparison.

Market results use only retained exact prospective player-game quotes with no-vig 1.5-over probability; they are descriptive and do not define betting filters. Fixed D20 is diagnostic only.

Existing divergence correction remains independently useful. Soft compression adds incremental value only if its paired gains are consistent in the tail and do not transfer into positive residual overcorrection. Prospective implementation justified: **True**.

## Feature decomposition and preserved backlog

D10 remains a core contributor. Current result: `CORE_CONTRIBUTOR_WITH_TWO_CORRECTABLE_BIASES`. Divergence correction remains unchanged; high-rate compression is research-only and no operational shadow was created.

- Historical official-TOI binding.
- Prospective grading of existing `NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1`.
- Continue fixed-blend prospective collection.
- Puck Line work deferred until tomorrow.
- Reusable cross-prop sanity-check / explanation audit, beginning with Points.

Provider calls: 0. Paid credits: 0. Production DB mutations: 0. Production and the existing divergence shadow were not modified.
