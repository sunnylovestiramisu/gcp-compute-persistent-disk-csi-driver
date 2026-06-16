OSS_DIR=/usr/local/google/home/elijahrb/go/src/sigs.k8s.io/gcp-compute-persistent-disk-csi-driver
G3_WS=manual-testing
export COMPONENT_VERSION=9999.9999.9999
export CLUSTER_VERSION=1.36.0-gke.1003000

#
# Clean up artifact registry
#

# Delete latest pdcsi image
gcloud artifacts docker images delete us-central1-docker.pkg.dev/elijahrb-gke-dev/csi-dev/gcp-compute-persistent-disk-csi-driver:latest --quiet
gcloud artifacts docker images delete us-central1-docker.pkg.dev/gke-elijahrb-hosted-master/csi-dev/gcp-compute-persistent-disk-csi-driver:latest --quiet

# Delete component related iamges
gcloud artifacts docker images delete us-central1-docker.pkg.dev/elijahrb-gke-dev/gke-cohort-images/pdcsi --delete-tags
gcloud artifacts docker images delete \
  us-central1-docker.pkg.dev/elijahrb-gke-dev/gke-cohortsets/gke-version --delete-tags --quiet
gcloud artifacts docker images delete \
  us-central1-docker.pkg.dev/elijahrb-gke-dev/gke-component-images/pdcsi --delete-tags --quiet

#
# Push local OSS changes
#
(
cd $OSS_DIR

# Push image for control plane changes
docker buildx build \
--build-arg STAGINGIMAGE=us-central1-docker.pkg.dev/gke-elijahrb-hosted-master/csi-dev \
--build-arg TARGETPLATFORM=linux/amd64  \
--build-arg STAGINGVERSION=gcp-compute-persistent-disk-csi-driver \
-t us-central1-docker.pkg.dev/gke-elijahrb-hosted-master/csi-dev/gcp-compute-persistent-disk-csi-driver . --push

# Push image for node changes
docker buildx build \
--build-arg STAGINGVERSION=us-central1-docker.pkg.dev/elijahrb-gke-dev/csi-dev \
--build-arg TARGETPLATFORM=linux/amd64  \
--build-arg STAGINGVERSION=gcp-compute-persistent-disk-csi-driver \
-t us-central1-docker.pkg.dev/elijahrb-gke-dev/csi-dev/gcp-compute-persistent-disk-csi-driver .  --push
)
#
# Build new component to with OSS changes
#

hgd $G3_WS

# Update pdcsi node image
sed -i "s|{{.imgPdcsiDriverNode}}|us-central1-docker.pkg.dev/elijahrb-gke-dev/csi-dev/gcp-compute-persistent-disk-csi-driver:latest|" cloud/kubernetes/distro/components/pdcsi/1.36/pdcsi_node.yaml
# Update pdcsi controller image
sed -i "s|{{.imgPdcsiDriverMaster}}|us-central1-docker.pkg.dev/gke-elijahrb-hosted-master/csi-dev/gcp-compute-persistent-disk-csi-driver:latest|" cloud/kubernetes/distro/components/pdcsi/1.36/pdcsi_controller.yaml

# Update component-builder

# Bump the component version
sed -i 's/\(version:\).*/\1 '"$COMPONENT_VERSION"'/' cloud/kubernetes/distro/components/pdcsi/1.35/component-builder.yaml

blaze run //cloud/kubernetes/distro/exporter -- --nodry_run --components=pdcsi

#
# Restart sandbox cluster server with local changes
#

blaze run //cloud/kubernetes/distro/pika:pika component test -- --name=pdcsi --version=$COMPONENT_VERSION --cohortbuilder="cloud/kubernetes/distro/cohort/pdcsi/cohort_builder.textpb" --cluster-version=$CLUSTER_VERSION --rebuild-cluster-server --skip-cluster

#
# Create a cluster to test cluster server creation changes (optional for OSS changes)
#

CLUSTER_COUNTER=0

CLUSTER_COUNTER=$((CLUSTER_COUNTER+1))
setup_gke_dev.sh --make-cluster --cluster-config "{
  \"name\": \"c${CLUSTER_COUNTER}\",
  \"nodePools\": [
    {
      \"config\": {
        \"oauthScopes\": [
          \"https://www.googleapis.com/auth/devstorage.read_only\",
          \"https://www.googleapis.com/auth/logging.write\",
          \"https://www.googleapis.com/auth/monitoring\",
          \"https://www.googleapis.com/auth/service.management.readonly\",
          \"https://www.googleapis.com/auth/servicecontrol\",
          \"https://www.googleapis.com/auth/trace.append\"
        ]
      },
      \"initialNodeCount\": 1,
      \"management\": {\"autoRepair\": true, \"autoUpgrade\": true},
      \"name\": \"default-pool\"
    }
  ],
  \"initialClusterVersion\": \"${CLUSTER_VERSION}\",
  \"network\": \"default3\",
}"
CLUSTER_HASH=$(kap cluster list --project=elijahrb-gke-dev --env=dev | tail -n1 | awk '{print $NF}')

MASTER_PROJECT=$(kap cluster info get --hash=$CLUSTER_HASH --env=dev | grep "Master Project" | awk '{print $NF}')
gcloud projects add-iam-policy-binding gke-elijahrb-hosted-master \
    --member="serviceAccount:kcp-vm@${MASTER_PROJECT}.iam.gserviceaccount.com" \
    --role="roles/artifactregistry.reader"
echo $MASTER_PROJECT
echo kcp-vm@$MASTER_PROJECT.iam.gserviceaccount.com
kap cluster components get --hash $CLUSTER_HASH --env=dev --components=gke-common-webhooks,pdcsi

echo https://pantheon-hourly.corp.google.com/logs/query;query=resource.labels.container_name%3D%22gce-pd-driver%22;referrer=search&project=$MASTER_PROJECT

# Restart pods to pull latest OSS image. This only needs to be done if using the same cluster.
kubectl delete pods -n kube-system -l="k8s-app=gcp-compute-persistent-disk-csi-driver"