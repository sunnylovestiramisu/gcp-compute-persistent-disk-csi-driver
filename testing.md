# GKE Sandbox Testing Guide for songsunny

This is an updated version of testing.md tailored for your development environment.

## Environment Variables

Run these commands in your shell to set up your environment:

```bash
# Local OSS repository directory
export OSS_DIR=/usr/local/google/home/songsunny/go/src/sigs.k8s.io/gcp-compute-persistent-disk-csi-driver

# Google-internal (g3) monorepo workspace name
export G3_WS=manual-testing

# Version configuration for GKE component
export COMPONENT_VERSION=9999.9999.9999
export CLUSTER_VERSION=1.36.0-gke.1003000

# Project definitions
export DEV_PROJECT=songsunny-gke-dev2
export HOSTED_MASTER_PROJECT=gke-songsunny-hm-dev2
```

---

## 0. Prerequisites: Create Artifact Registry Repositories

If you do not have the necessary Artifact Registry repositories in your dev and sandbox projects, run these commands to create them:

```bash
# 1. Create csi-dev repository in DEV_PROJECT (songsunny-gke-dev2)
gcloud artifacts repositories create csi-dev \
    --repository-format=docker \
    --location=us-central1 \
    --description="GKE PD CSI Driver Dev Repository" \
    --project=${DEV_PROJECT}

# 2. Create csi-dev repository in HOSTED_MASTER_PROJECT (gke-songsunny-hm-dev2)
gcloud artifacts repositories create csi-dev \
    --repository-format=docker \
    --location=us-central1 \
    --description="GKE PD CSI Driver Sandbox Repository" \
    --project=${HOSTED_MASTER_PROJECT}

# 3. Create GKE Component & Cohort repositories in DEV_PROJECT (needed by GKE pika tool)
gcloud artifacts repositories create gke-component-images \
    --repository-format=docker \
    --location=us-central1 \
    --description="GKE Component Images" \
    --project=${DEV_PROJECT}

gcloud artifacts repositories create gke-cohort-images \
    --repository-format=docker \
    --location=us-central1 \
    --description="GKE Cohort Images" \
    --project=${DEV_PROJECT}

gcloud artifacts repositories create gke-cohortsets \
    --repository-format=docker \
    --location=us-central1 \
    --description="GKE Cohort Sets" \
    --project=${DEV_PROJECT}
```

---

## 1. Clean Up Existing Artifact Registry Images

Before pushing new images, delete any previously built stale images:

```bash
# Delete latest pdcsi images
gcloud artifacts docker images delete us-central1-docker.pkg.dev/${DEV_PROJECT}/csi-dev/gcp-compute-persistent-disk-csi-driver:latest --quiet
gcloud artifacts docker images delete us-central1-docker.pkg.dev/${HOSTED_MASTER_PROJECT}/csi-dev/gcp-compute-persistent-disk-csi-driver:latest --quiet

# Delete component related images
gcloud artifacts docker images delete us-central1-docker.pkg.dev/${DEV_PROJECT}/gke-cohort-images/pdcsi --delete-tags --quiet
gcloud artifacts docker images delete us-central1-docker.pkg.dev/${DEV_PROJECT}/gke-cohortsets/gke-version --delete-tags --quiet
gcloud artifacts docker images delete us-central1-docker.pkg.dev/${DEV_PROJECT}/gke-component-images/pdcsi --delete-tags --quiet
```

---

## 2. Build and Push Local OSS Changes

Build the docker images from your local workspace and push them to your Artifact Registry:

```bash
(
cd $OSS_DIR

# Build & Push image for control plane (Controller) changes
docker buildx build \
  --build-arg STAGINGIMAGE=us-central1-docker.pkg.dev/${HOSTED_MASTER_PROJECT}/csi-dev \
  --build-arg TARGETPLATFORM=linux/amd64  \
  --build-arg STAGINGVERSION=gcp-compute-persistent-disk-csi-driver \
  -t us-central1-docker.pkg.dev/${HOSTED_MASTER_PROJECT}/csi-dev/gcp-compute-persistent-disk-csi-driver:latest . --push

# Build & Push image for node (DaemonSet) changes
docker buildx build \
  --build-arg STAGINGVERSION=us-central1-docker.pkg.dev/${DEV_PROJECT}/csi-dev \
  --build-arg TARGETPLATFORM=linux/amd64  \
  --build-arg STAGINGVERSION=gcp-compute-persistent-disk-csi-driver \
  -t us-central1-docker.pkg.dev/${DEV_PROJECT}/csi-dev/gcp-compute-persistent-disk-csi-driver:latest . --push
)
```

---

## 3. Build a New GKE Component with OSS Changes

Switch to your Google-internal Citc/hg workspace to compile the new GKE component:

```bash
# Navigate to your g3 workspace
hgd $G3_WS

# Point the component manifests to your freshly pushed development images
sed -i "s|{{.imgPdcsiDriverNode}}|us-central1-docker.pkg.dev/${DEV_PROJECT}/csi-dev/gcp-compute-persistent-disk-csi-driver:latest|" cloud/kubernetes/distro/components/pdcsi/1.36/pdcsi_node.yaml
# Update pdcsi controller image
sed -i "s|{{.imgPdcsiDriverMaster}}|us-central1-docker.pkg.dev/${HOSTED_MASTER_PROJECT}/csi-dev/gcp-compute-persistent-disk-csi-driver:latest|" cloud/kubernetes/distro/components/pdcsi/1.36/pdcsi_controller.yaml

# Bump the component version to force an upgrade
sed -i 's/\(version:\).*/\1 '"$COMPONENT_VERSION"'/' cloud/kubernetes/distro/components/pdcsi/1.36/component-builder.yaml

# Run the exporter to package the component
blaze run //cloud/kubernetes/distro/exporter -- --nodry_run --components=pdcsi
```

---

## 4. Restart Sandbox Cluster Server

Rebuild the GKE sandbox cluster server (control plane) with your new component:

```bash
blaze run //cloud/kubernetes/distro/pika:pika component test -- \
  --name=pdcsi \
  --version=$COMPONENT_VERSION \
  --cohortbuilder="cloud/kubernetes/distro/cohort/pdcsi/cohort_builder.textpb" \
  --cluster-version=$CLUSTER_VERSION \
  --rebuild-cluster-server \
  --skip-cluster \
  --project=${DEV_PROJECT} \
  --master-project=${HOSTED_MASTER_PROJECT}
```

---

## 5. Provision a Cluster & Node Pool with Data Cache Support

To support your tests, you need a node pool created with GKE Data Cache enabled (`--data-cache-count=1` / `dataCacheCount`). 

You can do this using either **Option A** (if creating the cluster from scratch) or **Option B** (if adding the pool to an existing cluster).

### Option A: Create a New Cluster from Scratch (via setup_gke_dev.sh)
If you want the cluster creation script to configure the datacache pool automatically, run:

```bash
CLUSTER_COUNTER=1

setup_gke_dev.sh --make-cluster --cluster-config "{
  \"name\": \"c${CLUSTER_COUNTER}\",
  \"nodePools\": [
    {
      \"config\": {
        \"machineType\": \"n2-standard-2\",
        \"ephemeralStorageLocalSsdConfig\": {
          \"dataCacheCount\": 1
        },
        \"oauthScopes\": [
          \"https://www.googleapis.com/auth/devstorage.read_only\",
          \"https://www.googleapis.com/auth/logging.write\",
          \"https://www.googleapis.com/auth/monitoring\",
          \"https://www.googleapis.com/auth/service.management.readonly\",
          \"https://www.googleapis.com/auth/servicecontrol\",
          \"https://www.googleapis.com/auth/trace.append\"
        ]
      },
      \"initialNodeCount\": 2,
      \"management\": {\"autoRepair\": true, \"autoUpgrade\": true},
      \"name\": \"datacache-node-pool\"
    }
  ],
  \"initialClusterVersion\": \"${CLUSTER_VERSION}\",
  \"network\": \"default\"
}"

# Get the GKE cluster hash and Master Project
CLUSTER_HASH=$(kap cluster list --project=${DEV_PROJECT} --env=dev | tail -n1 | awk '{print $NF}')
```

---

### Option B: Add a Datacache Node Pool to an Existing Cluster (via gcloud)
If you already have a cluster running, you can add the datacache node pool directly using the `gcloud` command:

```bash
# Get your cluster name and location
CLUSTER_NAME=c1 # Update if your cluster name is different
LOCATION=us-central1-a # Update with your cluster's zone

# Create the node pool with data cache configured
gcloud container node-pools create datacache-node-pool \
    --cluster=${CLUSTER_NAME} \
    --location=${LOCATION} \
    --num-nodes=2 \
    --data-cache-count=1 \
    --machine-type=n2-standard-2 \
    --project=${DEV_PROJECT}

# Get the GKE cluster hash
CLUSTER_HASH=$(kap cluster list --project=${DEV_PROJECT} --env=dev | tail -n1 | awk '{print $NF}')
```

---

### Apply IAM Permissions & Fetch Components

Once the cluster and node pool are ready, apply the master project IAM reader permissions and fetch the components:

```bash
# Get the Master Project
MASTER_PROJECT=$(kap cluster info get --hash=$CLUSTER_HASH --env=dev | grep "Master Project" | awk '{print $NF}')

# Grant the master project's service account permission to read your dev project's Artifact Registry
gcloud projects add-iam-policy-binding ${HOSTED_MASTER_PROJECT} \
    --member="serviceAccount:kcp-vm@${MASTER_PROJECT}.iam.gserviceaccount.com" \
    --role="roles/artifactregistry.reader"

# Push the new components to the cluster
kap cluster components get --hash $CLUSTER_HASH --env=dev --components=gke-common-webhooks,pdcsi
```

---

## 6. Apply Changes to an Existing Cluster (Fast Iteration)

If you are using an existing cluster and just want to pull the latest image you built in **Step 2**:

```bash
# Force Kubernetes to recreate the pods and pull your newly pushed ":latest" image
kubectl delete pods -n kube-system -l="k8s-app=gcp-compute-persistent-disk-csi-driver"
```

---

## Log Inspection Link

You can monitor the CSI driver logs on your master project here:
```text
https://pantheon-hourly.corp.google.com/logs/query;query=resource.labels.container_name%3D%22gce-pd-driver%22;referrer=search&project=${MASTER_PROJECT}
```