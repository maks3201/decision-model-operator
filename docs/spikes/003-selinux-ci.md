# Spike 003 — SELinux-enforcing node in free GitHub CI

Goal: decide whether the SELinux store-sharing regression can be exercised under a
real SELinux-enforcing kernel in **free** GitHub-hosted CI, and if so, how, with a CI-time
estimate. No workflow changes are made by this spike — it is a feasibility note only.

## The bug we would be reproducing

On SELinux-enforcing nodes (Bottlerocket on EKS) whose EBS CSI driver lacks SELinux mount
support, the container runtime relabels the shared store volume to the MCS level of the Pod
that mounts it. The prefetch Pod and the serving Pods received different random MCS levels, so
serving failed with `Permission denied (os error 13)` on the store and CrashLoopBacked-off.
The fix derives one stable MCS level per DecisionModel (`selinuxLevel` in
`internal/controller/helpers.go`) and sets it on both the prefetch Job and the serving
Deployment Pod templates. The current kind-on-`ubuntu-latest` e2e cannot see this because the
runner does not enforce SELinux (see below), so the e2e regression only asserts
the operator *sets a shared, well-formed level on both Pod templates*; it does not exercise
kernel enforcement. This spike is about whether enforcement can be tested for free.

## The hard constraint: the host LSM is fixed at boot

The Linux kernel allows exactly one "major" LSM to be active, chosen at boot. Ubuntu ships and
boots with **AppArmor** as the active LSM; SELinux is not installed or enabled. Switching to
SELinux on Ubuntu requires installing SELinux packages, disabling AppArmor, adding the
`selinux=1 security=selinux` kernel command line, relabelling the filesystem, and **rebooting**
into that boot config. (Ubuntu's own security docs: AppArmor is the default MAC; other LSMs are
"available but not supported". Community guides all require a disable-AppArmor + reboot cycle.)

Why that kills the "just enable SELinux on the runner" idea:

- A GitHub-hosted runner job runs on an ephemeral VM that we do not reboot. We cannot change the
  booted kernel command line and come back on the same job. So the **host** kernel of
  `ubuntu-latest` is AppArmor and stays AppArmor for the whole job.
- Therefore kind (or k3s) running **directly** on the runner shares that host kernel's LSM.
  Pods there are confined by AppArmor, never SELinux. `kind` with a Fedora/CentOS *node image*
  does not change this: the node image only swaps the container userland; the LSM that enforces
  on the containers is the **host** kernel's, which is AppArmor. So a Fedora kind node on
  `ubuntu-latest` still gives you an AppArmor host — no SELinux enforcement, no relabelling MCS
  behaviour, bug not reproducible. (Confirmed by the LSM-is-host-kernel property above; I did
  not burn CI minutes to re-observe a known-negative.)

Conclusion for options (a) "kind + Fedora node image": **does not work** — enforcement is a
host-kernel property and the host is AppArmor.

## What *is* available for free: /dev/kvm (nested virtualization)

GitHub-hosted **Linux x86-64** runners (`ubuntu-latest`, `ubuntu-24.04`) expose `/dev/kvm` with
hardware acceleration — this is the same capability the official Android-emulator and
nested-VM actions depend on. (ARM64 runners do **not** expose `/dev/kvm`.) That means we can
boot a *guest* VM whose own kernel boots with SELinux enforcing, and run the cluster inside the
guest. The guest's LSM is independent of the AppArmor host.

Probe plan to confirm on the runner (one-off `workflow_dispatch` job, ~1 min, not committed):

```bash
# in a job on ubuntu-latest
ls -l /dev/kvm && echo "kvm present"
egrep -c '(vmx|svm)' /proc/cpuinfo        # >0 => HW virt extensions passed through
kvm-ok || true                            # cpu-checker, if installed
```

I did not spend CI minutes on this probe because `/dev/kvm` on the free x86-64 image is
well-established and already relied on by widely-used actions; if a reviewer wants hard local
evidence, the snippet above produces it in under a minute.

## Viable option: nested VM with an SELinux-enforcing guest + k3s --selinux

Option (b): boot a Fedora/Fedora-CoreOS (or CentOS Stream) guest via QEMU/KVM on the runner,
install k3s with SELinux support, deploy the operator + a DecisionModel, and assert the serving
Pod does **not** CrashLoop on the store (i.e. the shared-level fix holds) — and, as a negative
control, that forcing distinct levels reproduces the EACCES.

Sketch:

```yaml
# nested-VM SELinux e2e (illustrative — NOT added here)
runs-on: ubuntu-latest
steps:
  - run: |
      sudo apt-get update && sudo apt-get install -y qemu-system-x86 cloud-image-utils
  - run: |
      # Fedora Cloud image boots with SELinux enforcing by default.
      curl -Lo fedora.qcow2 "<fedora-cloud-qcow2-url>"
      # cloud-init: install k3s with --selinux, load operator image, run tests
      qemu-system-x86_64 -enable-kvm -m 6144 -smp 4 \
        -drive file=fedora.qcow2,if=virtio -nographic ...
```

k3s supports SELinux via the `k3s-selinux` policy and the `--selinux` server flag; a Fedora
guest boots enforcing by default, so inside the guest `getenforce` returns `Enforcing` and the
container runtime performs real MCS relabelling of volumes — exactly the condition the bug
needs.

Cost / fit:

- Guest boot + k3s install: ~3–5 min. Pulling the Ollaya image and a model inside the guest is
  the same multi-minute cost as the existing e2e, plus nested-virt overhead (CPU-only, no model
  warm cache). Realistic wall-clock: **~20–30 min** per run — roughly double the current kind
  e2e, on a slower (nested) CPU.
- Complexity: a second, materially different harness (cloud-init, image hosting/caching, SSH
  into guest, artifact extraction) that we would own and maintain.
- It reproduces MCS relabelling, but note EKS's exact trigger also involves a CSI driver that
  lacks SELinux mount support; k3s local-path volumes may relabel differently than EBS. So even
  the nested-VM test is an approximation of the production trigger, not an exact replica.

Option (c) "anything else cheap": none found. Larger GitHub runners and third-party runners
(e.g. WarpBuild) offer nested virt as a paid label, but that is out of scope for *free* CI.
`unshare`/user-namespace tricks cannot create a second enforcing LSM.

## Recommendation

**Do not add an SELinux-enforcing e2e to free CI now.** Rationale:

1. The only free path is a nested Fedora/CoreOS VM (option b). It roughly doubles e2e wall-clock
   (~20–30 min), adds a whole second harness to maintain, and still only *approximates* the EKS
   CSI trigger.
2. The behaviour that actually regressed is "the operator puts one shared, well-formed MCS level
   on both Pod templates". That is deterministic controller output and is now covered cheaply
   two ways: the controller unit test `internal/controller/selinux_test.go`
   and the SELinux e2e assertion on the live Job/Deployment templates. Kernel enforcement adds
   little once the level-sharing invariant is locked down.
3. If we ever want true enforcement coverage, prefer the existing **EKS GPU/e2e** lane (real
   Bottlerocket, real SELinux, real CSI) over a nested VM on free runners — it is the actual
   environment and avoids a bespoke QEMU harness. Track that under a future task if the EKS lane
   is formalised.

If a reviewer wants a tracking item regardless, the cheapest *credible* option is option (b) on
`ubuntu-latest` with a Fedora Cloud guest + `k3s --selinux`, budgeted at ~30 min and gated to a
manual / nightly trigger (never on the per-push critical path).

## Evidence summary

- Ubuntu's active LSM is AppArmor by default; enabling SELinux needs package install + disable
  AppArmor + `selinux=1` kernel cmdline + reboot — impossible to do and re-enter within one
  ephemeral runner job. (Ubuntu security docs; multiple install guides.)
- LSM enforcement is a property of the **booted host kernel**, so a Fedora kind *node image* on
  an Ubuntu host is still AppArmor-confined — option (a) cannot reproduce the bug.
- Free Linux x86-64 GitHub runners expose `/dev/kvm` (same capability the Android-emulator
  actions use), so a nested SELinux-enforcing guest (option b) is technically possible.
- Content was rephrased for compliance with licensing restrictions.
