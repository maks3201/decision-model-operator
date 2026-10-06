# vhs tape for the eval-gated rollout demo GIF.
#
# Re-record (run from the repo root):
#   1. Prepare a cluster:  make demo-prepare
#   2. Reset the DM state: kubectl --context kind-dmo-demo delete ns dmo-demo --ignore-not-found
#   3. Record:             vhs examples/demo/demo.tape
#
# The recorded command is the real demo (hack/demo.sh run) against a prepared
# cluster; every line in the GIF is live output. NO_WAIT=1 removes the interactive
# "press Enter" pauses so the capture runs unattended. PlaybackSpeed makes the GIF
# play back faster than wall-clock (the long per-revision prefetch + evaluation
# waits are real; the GIF is sped up ~8x to stay small and readable).
Output "docs/assets/demo.gif"

Set Shell "bash"
Set FontSize 13
Set Width 1200
Set Height 760
Set Padding 12
Set Framerate 6
Set PlaybackSpeed 8.0

Type "# decision-model-operator: a weak candidate is rejected, a strong one is promoted (sped up ~8x)"
Enter
Sleep 1s
Type "NO_WAIT=1 KIND_CLUSTER=dmo-demo hack/demo.sh run"
Enter
# Real wall-clock budget for a cold run (prefetch Jobs + two evaluations).
Sleep 320s
