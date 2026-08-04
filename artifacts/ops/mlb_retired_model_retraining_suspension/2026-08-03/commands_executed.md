# Commands executed

- `launchctl bootout gui/501/com.proppadia.mlb.retrain.weekly`
- `launchctl disable gui/501/com.proppadia.mlb.retrain.weekly`
- copied the original plist and wrapper into `originals/` before mutation
- installed a fail-closed wrapper preserving the original chain below an unconditional exit
- validated runtime load, trainer, recalibration gate, and bundle-publish gate
- compared all `models_out/latest` hashes before and after
