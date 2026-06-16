# Spot VM Preemption and Recovery for GKE Data Cache

This directory contains the manifests and documentation for simulating, executing, and verifying **Spot VM Preemption Recovery with Zero Data Loss** when using GKE Data Cache (Local SSDs caching a GCE Persistent Disk).

---

## 1. The Architectural Challenge: LVM Duplicate Name Deadlock

When a GKE node using Data Cache is preempted (Spot VM) and replaced by a Managed Instance Group (MIG) under the same node name:
1. The old VM's Local SSDs are physically destroyed and gone.
2. The GCE Persistent Disk (PD) is detached from the dead VM and attached to the brand-new VM.
3. The GCE PD retains the stale LVM Volume Group (VG) metadata containing the name `csi-vg-<node_hash>`.
4. The new VM's CSI driver boots up and attempts to initialize a *new* VG on its fresh Local SSDs, using the **same** name (`csi-vg-<node_hash>`).

This creates a **duplicate VG name conflict** in LVM.

### The Deadlock:
* **Cannot Rename by UUID**: LVM strictly refuses to rename a VG using `vgrename` if it has missing physical volumes (the old node's Local SSDs are gone).
* **Cannot Clean Up by Name**: We cannot run `vgreduce --removemissing` using the VG name because LVM sees two VGs with the same name and skips commands to avoid corruption.
* **Result**: The CSI driver fails to stage the volume, entering a terminal `DataLoss` mount error loop.

---

## 2. The Deadlock-Breaker Fix

The CSI driver resolves this conflict dynamically during the `NodeStageVolume` phase using a safe, three-step LVM recovery sequence isolated by **LVM Device Filtering**:

### The Sequence:
1. **Device Isolation**: The driver constructs a dynamic LVM config filter to isolate all scanning to ONLY the GCE PD (`/dev/sdX`), hiding the active Local SSDs:
   ```bash
   --config 'devices { filter = [ "a|/dev/sdX|", "r|.*|" ] }'
   ```
2. **Uncache First (Crucial for Zero Data Loss)**: It runs `lvconvert --uncache` under the device filter:
   ```bash
   lvconvert --uncache <stale_vg_name>/<lv_name> --force -y --config 'devices { filter = ... }'
   ```
   * *Why*: This safely decouples the missing SSD cache references while **preserving the origin logical volume (containing 100% of your persistent data) completely intact.**
3. **Reduce**: It runs `vgreduce --removemissing` under the filter to wipe the missing SSD references, restoring the stale VG's health:
   ```bash
   vgreduce --removemissing <stale_vg_name> --force --config 'devices { filter = ... }'
   ```
4. **Rename**: It renames the stale VG to a unique temporary name (`csi-vg-stale-<short_uuid>`) to permanently clear the namespace:
   ```bash
   vgrename <stale_vg_name> csi-vg-stale-<short_uuid> --config 'devices { filter = ... }'
   ```

Once the namespace is clean, the filter is released, and the driver cleanly configures the new Data Cache on the new Local SSDs!

---

## 3. Step-by-Step Preemption Test (Same Node Name, New VM ID)

Follow these steps to reproduce and test the preemption recovery flow:

### Step 1: Deploy the Workload
Deploy the `writethrough` StorageClass (guarantees every write is committed immediately to GCE PD) and the StatefulSet workload:
```bash
# Apply StorageClass and StatefulSet
kubectl apply -f 01-storageclass.yaml
kubectl apply -f 03-statefulset.yaml

# Wait for the pod to transition to Running
kubectl wait --for=condition=Ready pod/datacache-test-ss-0 --timeout=90s
```

### Step 2: Establish the Baseline
Verify that the pod is actively writing to the volume:
```bash
kubectl exec datacache-test-ss-0 -- tail -f /mnt/data/test.log
```
*Note down the last timestamp printed (e.g. `23:48:34 UTC`).*

### Step 3: Trigger Preemption (Option B)
1. **Cordon the surviving node** in the node pool to force the pod to schedule exclusively onto the replacement VM:
   ```bash
   kubectl cordon <surviving-node-name>
   ```
2. **Delete the active GCE VM instance** holding the pod to simulate a hard preemption:
   ```bash
   gcloud compute instances delete <active-node-name> \
       --zone=us-central1-c \
       --project=<project-id> \
       --quiet
   ```
3. **Delete the old node object** from Kubernetes to force-clear the old IP registration and trigger instant new VM registration:
   ```bash
   kubectl delete node <active-node-name>
   ```

---

## 4. Verification and Proof of Success

Once the replacement VM boots up and registers:

### 1. Pod Recovery Verification
Verify that the pod `datacache-test-ss-0` automatically schedules onto the new VM and transitions successfully to **`Running`**:
```bash
kubectl get pod datacache-test-ss-0 -o wide
```

### 2. Log Continuity Verification (Zero Data Loss Proof)
Print the contents of the log file inside the pod:
```bash
kubectl exec datacache-test-ss-0 -- cat /mnt/data/test.log
```
#### Expected Output:
You should see the complete log history spanning across the preemption. Note the two different gaps in the timestamps:
```text
Mon Jun 15 23:35:07 UTC 2026  <-- LAST ENTRY before GCE VM Preemption
... [13-Minute Gap: VM Boot, Node Join, Driver Patch & Redeployment] ...
Mon Jun 15 23:48:07 UTC 2026  <-- FIRST ENTRY after successful LVM recovery
...
Mon Jun 15 23:48:34 UTC 2026  <-- LAST ENTRY before manual pod delete
Mon Jun 15 23:48:38 UTC 2026  <-- FIRST ENTRY after pod recreation (Only 4s restart latency!)
Mon Jun 15 23:48:39 UTC 2026
```

#### What this proves:
1. **Zero Data Loss**: All log entries written before the preemption (`23:35:07`) were **100% preserved** on the GCE PD across the entire 13-minute preemption, node recreation, and driver rollout.
2. **Ultra-Low Mount Latency**: Once a healthy node is registered, the CSI driver mounts the volume and starts the pod container in **only 4 seconds**.
```

### 3. Driver Execution Verification
Locate the CSI Node pod running on the new VM and inspect its logs:
```bash
kubectl logs <csi-node-pod-name> -n gce-pd-csi-driver -c gce-pd-driver --tail=100
```
#### Expected Output:
You should see your device-filtered recovery sequence execute successfully:
```text
I0615 23:48:10.263781      11 cache.go:72] Uncaching stale volume csi-vg-dg9xw456/csi-main-pvc-... using device filter to preserve data
I0615 23:48:10.269325      11 cache.go:78] Cleaning up missing PVs on stale VG csi-vg-dg9xw456 using device filter
I0615 23:48:10.290131      11 cache.go:84] Renaming stale VG csi-vg-dg9xw456 to csi-vg-stale-31cTAD-9 using device filter
```
