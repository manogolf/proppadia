# Rollback

Installed-wrapper pre-change SHA-256:

`09d236d57eeff4576c97eac06c9fb8ad60a24f41e1091e1c3dccea6ead73faef`

Authorized post-change SHA-256:

`3f642baa92a7350b6fae9beeaaba3c26f425b90a688b36795f5258f00be520d2`

After confirming that no MLB wrapper is active, reverse only the tracked
wrapper fragment:

```sh
cd /Users/jerrystrain/bin
patch -R -p1 < /Users/jerrystrain/Projects/proppadia/docs/contracts/mlb_2026_prospective_exact_game_feature_shadow_writer_v1/installed_wrapper.patch
zsh -n /Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh
shasum -a 256 /Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh
```

The resulting hash must equal the pre-change hash above. Revert the bounded
repository commit separately. Do not delete natural shadow packages: they are
immutable operational evidence even after producer rollback.
