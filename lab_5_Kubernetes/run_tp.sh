#!/usr/bin/env bash
# Exécute les labs Kubernetes (lab-1, lab-2, lab-3) et consigne les VRAIES sorties
# dans RENDU_TP_K8S.md. Les réponses aux questions restent à rédiger à la main.
# Prérequis : minikube, kubectl, jq, curl. Lancer depuis le dossier lab_5_Kubernetes/.
set -uo pipefail

OUT="$(pwd)/RENDU_TP_K8S.md"
ROOT="$(pwd)"
WORK="$ROOT/work"
mkdir -p "$WORK"

md()      { printf '%s\n' "$*" >> "$OUT"; }
section() { md ""; md "$1"; md ""; echo ">>> $1"; }
answer()  { md ""; md "> **Réponse :** _à rédiger_"; md ""; }

# Exécute une commande, l'affiche et écrit commande + sortie dans le rendu.
run() {
  echo "\$ $*"
  local res
  res="$(bash -c "$*" 2>&1)"
  echo "$res"
  { echo '```console'; echo "\$ $*"; echo "$res"; echo '```'; } >> "$OUT"
}

cat > "$OUT" <<EOF
# TP Kubernetes : rendu

- Groupe : gr-XX
- Membres : NOM Prénom (git username)
- Date d'exécution : $(date '+%Y-%m-%d %H:%M')

Usage de l'IA : script d'exécution et complétion des manifests générés avec l'aide d'un assistant IA.
Les réponses aux questions sont rédigées par l'étudiant.
EOF

########################################################################
section "## Lab 1 : Kubernetes basics with minikube"
########################################################################

section "### 1. Install minikube"
run "minikube start"
run "minikube status"
run "kubectl get nodes"
run "kubectl describe node minikube"
run "minikube addons enable metrics-server"
echo "Attente du metrics-server (90 s)..."; sleep 90
run "kubectl top pods -A --sort-by cpu --sum=true"
md "**Question :** que fait la commande \`kubectl top pods -A --sort-by cpu --sum=true\` ?"
answer
run "kubectl -n kube-system get pods"
md "**Question :** rôle de chaque composant de \`kube-system\`."
answer

section "### 2. Learn to use kubectl commands"
run "kubectl create deployment kubernetes-bootcamp --image=gcr.io/google-samples/kubernetes-bootcamp:v1"
run "kubectl wait --for=condition=Available deployment/kubernetes-bootcamp --timeout=180s"
run "kubectl get pods"
POD_NAME=$(kubectl get pods -l app=kubernetes-bootcamp -o jsonpath='{.items[0].metadata.name}')
export POD_NAME
run "echo $POD_NAME"
run "kubectl logs $POD_NAME"
run "kubectl describe pod $POD_NAME"
run "kubectl exec $POD_NAME -- cat /etc/os-release"
md "Shell dans le pod (équivalent non interactif de \`kubectl exec -it \$POD_NAME -- bash\`) :"
run "kubectl exec $POD_NAME -- ls -la"
run "kubectl exec $POD_NAME -- cat /server.js"
md "**Question :** où se trouve le code source JavaScript et sur quel port répond l'application ?"
answer
run "kubectl exec $POD_NAME -- curl -s localhost:8080"
md "**Question :** est-il possible d'interroger l'application depuis la machine locale, hors du pod ?"
run "curl -s --max-time 3 localhost:8080 || echo 'Echec de la connexion'"
answer

section "### 3. Expose a Kubernetes service outside the cluster"
run "kubectl expose deployments/kubernetes-bootcamp --type=NodePort --port 8080"
run "kubectl get services"
run "minikube ip"
# Avec le driver docker, l'IP du noeud n'est pas joignable : on passe par le tunnel.
minikube service kubernetes-bootcamp --url > "$WORK/url.txt" 2>/dev/null &
TUNNEL_PID=$!
for _ in $(seq 30); do grep -q http "$WORK/url.txt" 2>/dev/null && break; sleep 1; done
APP_URL=$(grep -m1 http "$WORK/url.txt")
export APP_URL
run "echo $APP_URL"
run "curl -s $APP_URL"

section "### 4. Scale up and down a deployment"
run "kubectl scale deployments/kubernetes-bootcamp --replicas=5"
run "kubectl rollout status deployments/kubernetes-bootcamp --timeout=120s"
run "kubectl get pods -o wide"
md "**Question :** quelle commande pour vérifier les 5 pods ?"
answer
run "for i in \$(seq 10); do curl -s $APP_URL; done"
md "**Question :** que se passe-t-il ? Pourquoi ?"
answer
run "kubectl scale deployments/kubernetes-bootcamp --replicas=2"
sleep 20
run "kubectl get pods"

section "### 5. Rolling update and rollback"
( while true; do echo "$(date +%T) $(curl -s --max-time 2 "$APP_URL")"; sleep 0.5; done ) > "$WORK/curl_loop.log" 2>&1 &
LOOP_PID=$!
run "kubectl set image deployments/kubernetes-bootcamp kubernetes-bootcamp=jocatalin/kubernetes-bootcamp:v2"
run "kubectl rollout status deployments/kubernetes-bootcamp --timeout=180s"
sleep 5
md "Extrait de la boucle curl pendant la mise à jour v1 → v2 :"
run "cat $WORK/curl_loop.log | tail -40"
md "**Question :** qu'est-il arrivé aux réponses ? Le service a-t-il été interrompu ?"
answer
: > "$WORK/curl_loop.log"
run "kubectl set image deployments/kubernetes-bootcamp kubernetes-bootcamp=jocatalin/kubernetes-bootcamp:v3"
sleep 45
run "kubectl get pods"
run "kubectl rollout status deployments/kubernetes-bootcamp --timeout=10s"
run "tail -10 $WORK/curl_loop.log"
md "**Question :** que se passe-t-il ? Le service répond-il toujours ? Pourquoi ?"
answer
run "kubectl rollout undo deployments/kubernetes-bootcamp"
run "kubectl rollout status deployments/kubernetes-bootcamp --timeout=120s"
run "kubectl rollout history deployments/kubernetes-bootcamp"
REV_V1=$(for r in $(kubectl rollout history deployments/kubernetes-bootcamp | awk 'NR>2 && $1 ~ /^[0-9]+$/ {print $1}'); do
  kubectl rollout history deployments/kubernetes-bootcamp --revision="$r" | grep -q 'kubernetes-bootcamp:v1' && echo "$r"; done | tail -1)
md "**Question :** comment revenir à l'image choisie en partie 2 (v1) ?"
answer
run "kubectl rollout undo deployments/kubernetes-bootcamp --to-revision=$REV_V1"
run "kubectl rollout status deployments/kubernetes-bootcamp --timeout=120s"
run "kubectl get deployment kubernetes-bootcamp -o jsonpath='{.spec.template.spec.containers[0].image}'"
run "curl -s $APP_URL"
kill "$LOOP_PID" 2>/dev/null

section "### 6. Deploy an app using manifest yaml files"
kill "$TUNNEL_PID" 2>/dev/null
run "kubectl delete service kubernetes-bootcamp"
run "kubectl delete deployment kubernetes-bootcamp"
cd "$ROOT"
# Etape 3 : deployment sans le champ replicas (TO COMPLETE #1 seulement)
grep -v 'replicas:' deployment.yaml > "$WORK/deployment-step1.yaml"
run "cat $WORK/deployment-step1.yaml"
run "kubectl apply -f $WORK/deployment-step1.yaml"
run "kubectl wait --for=condition=Available deployment/kubernetes-bootcamp --timeout=120s"
run "kubectl get pods"
md "**Question :** les pods tournent-ils ?"
answer
run "cat service.yaml"
run "kubectl apply -f service.yaml"
run "kubectl get services"
minikube service kubernetes-bootcamp-service --url > "$WORK/url2.txt" 2>/dev/null &
TUNNEL_PID=$!
for _ in $(seq 30); do grep -q http "$WORK/url2.txt" 2>/dev/null && break; sleep 1; done
APP_URL=$(grep -m1 http "$WORK/url2.txt")
run "curl -s $APP_URL"
md "**Question :** le service est-il accessible ?"
answer
run "cat deployment.yaml"
run "kubectl apply -f deployment.yaml"
run "kubectl rollout status deployment/kubernetes-bootcamp --timeout=120s"
run "kubectl get pods"
run "for i in \$(seq 10); do curl -s $APP_URL; done"
md "**Question :** touche-t-on des réplicas différents ?"
answer

section "### 7. Teardown"
kill "$TUNNEL_PID" 2>/dev/null
run "kubectl delete -f service.yaml"
run "kubectl delete -f deployment.yaml"
cd "$ROOT"
# minikube n'est pas arrêté ici : les labs 2 et 3 en ont besoin (arrêt en fin de script).

########################################################################
section "## Lab 2 : ConfigMap and Secret"
########################################################################
NS2=lab-configmap-secret
mkdir -p "$WORK/$NS2" && cd "$WORK/$NS2"
run "kubectl create namespace $NS2"

section "### 2. Creating ConfigMaps"
cat > pipeline-config.properties <<'EOF'
# Data Pipeline Configuration
bronze.path=/mnt/data/bronze
silver.path=/mnt/data/silver
gold.path=/mnt/data/gold
batch.size=1000
retention.days=90
log.level=INFO
spark.cores=4
kafka.brokers=kafka-0.kafka.svc.cluster.local:9092
EOF
run "kubectl -n $NS2 create configmap pipeline-config --from-file=pipeline-config.properties"
run "kubectl -n $NS2 get configmap pipeline-config"
run "kubectl -n $NS2 describe configmap pipeline-config"
run "kubectl -n $NS2 get configmap pipeline-config -o yaml"
run "kubectl -n $NS2 create configmap data-paths --from-literal=bronze=/mnt/data/bronze --from-literal=silver=/mnt/data/silver --from-literal=gold=/mnt/data/gold"
run "kubectl -n $NS2 get configmap data-paths -o yaml"
cat > configmap-data-platform.yaml <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: data-platform-config
  namespace: lab-configmap-secret
data:
  log_level: INFO
  spark_workers: "4"
  batch_size: "1000"
  application.yaml: |
    # Data Platform Application Config
    pipeline:
      name: bronze-to-silver-etl
      schedule: "0 2 * * *"
      timeout_minutes: 120
    storage:
      type: s3
      endpoint: ceph-rgw.ceph-storage.svc.cluster.local:7480
      region: us-east-1
    kafka:
      bootstrap_servers: kafka-0.kafka.svc.cluster.local:9092
      consumer_group: data-platform-consumers
      topic_prefix: data.bronze
  table_mappings.csv: |
    source_table,bronze_path,silver_layer
    customers,/data/bronze/customers,deduplicated_customers
    orders,/data/bronze/orders,aggregated_orders
    products,/data/bronze/products,product_catalog
EOF
run "kubectl apply -f configmap-data-platform.yaml"
run "kubectl -n $NS2 get configmap data-platform-config -o yaml"
run "kubectl -n $NS2 get configmap data-platform-config -o jsonpath='{.data.log_level}'"
run "kubectl -n $NS2 get configmap data-platform-config -o jsonpath='{.data.application\.yaml}'"

section "### 3. Creating Secrets"
run "kubectl -n $NS2 create secret generic ceph-credentials --from-literal=access-key=AKIAIOSFODNN7EXAMPLE --from-literal=secret-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
run "kubectl -n $NS2 get secret ceph-credentials -o yaml"
cat > s3-credentials.txt <<'EOF'
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
EOF
run "kubectl -n $NS2 create secret generic s3-credentials --from-file=s3-credentials.txt"
run "kubectl -n $NS2 get secret s3-credentials -o yaml"
cat > secret-ceph-credentials.yaml <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: ceph-s3-credentials
  namespace: lab-configmap-secret
type: Opaque
stringData:
  access-key: AKIAIOSFODNN7EXAMPLE
  secret-key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  endpoint: ceph-rgw.ceph-storage.svc.cluster.local:7480
  bucket: data-platform-bronze
EOF
run "kubectl apply -f secret-ceph-credentials.yaml"
run "kubectl -n $NS2 get secret ceph-s3-credentials -o yaml"
run "kubectl -n $NS2 get secret ceph-s3-credentials -o jsonpath='{.data.secret-key}' | base64 -d"
run "kubectl -n $NS2 get secret ceph-s3-credentials -o json | jq -r '.data | to_entries[] | \"\(.key)=\(.value | @base64d)\"'"

section "### 4. Using ConfigMap & Secret in Pods"
cat > pod-with-configmap.yaml <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: spark-app
  namespace: lab-configmap-secret
spec:
  containers:
  - name: app
    image: busybox:1.37
    command: ["sleep", "600"]
    env:
    - name: LOG_LEVEL
      valueFrom:
        configMapKeyRef:
          name: data-platform-config
          key: log_level
    - name: SPARK_WORKERS
      valueFrom:
        configMapKeyRef:
          name: data-platform-config
          key: spark_workers
    envFrom:
    - configMapRef:
        name: data-platform-config
EOF
run "kubectl apply -f pod-with-configmap.yaml"
run "kubectl -n $NS2 wait --for=condition=Ready pod/spark-app --timeout=90s"
run "kubectl -n $NS2 exec spark-app -- env | grep -iE 'log_level|spark'"

cat > pod-with-secret.yaml <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: ceph-connector
  namespace: lab-configmap-secret
spec:
  containers:
  - name: ceph-app
    image: busybox:1.37
    command: ["sh", "-c", "echo \"Access Key: $S3_ACCESS_KEY\""]
    env:
    - name: S3_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: ceph-s3-credentials
          key: access-key
    - name: S3_SECRET_KEY
      valueFrom:
        secretKeyRef:
          name: ceph-s3-credentials
          key: secret-key
    - name: S3_ENDPOINT
      valueFrom:
        secretKeyRef:
          name: ceph-s3-credentials
          key: endpoint
    envFrom:
    - secretRef:
        name: ceph-s3-credentials
  restartPolicy: Never
EOF
run "kubectl apply -f pod-with-secret.yaml"
run "kubectl -n $NS2 wait --for=jsonpath='{.status.phase}'=Succeeded pod/ceph-connector --timeout=90s"
run "kubectl -n $NS2 logs ceph-connector"

cat > pod-with-configmap-volume.yaml <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: spark-config-reader
  namespace: lab-configmap-secret
spec:
  containers:
  - name: app
    image: busybox:1.37
    command: ["sleep", "600"]
    volumeMounts:
    - name: config-volume
      mountPath: /etc/config
  volumes:
  - name: config-volume
    configMap:
      name: data-platform-config
      items:
      - key: application.yaml
        path: application.yaml
      - key: table_mappings.csv
        path: tables.csv
EOF
run "kubectl apply -f pod-with-configmap-volume.yaml"
run "kubectl -n $NS2 wait --for=condition=Ready pod/spark-config-reader --timeout=90s"
run "kubectl -n $NS2 exec spark-config-reader -- ls -la /etc/config/"
run "kubectl -n $NS2 exec spark-config-reader -- cat /etc/config/application.yaml"
run "kubectl -n $NS2 exec spark-config-reader -- cat /etc/config/tables.csv"

cat > pod-with-secret-volume.yaml <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: ceph-connector-secure
  namespace: lab-configmap-secret
spec:
  containers:
  - name: ceph-app
    image: busybox:1.37
    command: ["sh", "-c", "sleep 1000"]
    volumeMounts:
    - name: ceph-creds
      mountPath: /etc/ceph-credentials
      readOnly: true
  volumes:
  - name: ceph-creds
    secret:
      secretName: s3-credentials
      defaultMode: 0600
EOF
run "kubectl apply -f pod-with-secret-volume.yaml"
run "kubectl -n $NS2 wait --for=condition=Ready pod/ceph-connector-secure --timeout=90s"
run "kubectl -n $NS2 exec ceph-connector-secure -- ls -laL /etc/ceph-credentials/"
run "kubectl -n $NS2 exec ceph-connector-secure -- cat /etc/ceph-credentials/s3-credentials.txt"

section "### Teardown"
run "kubectl delete namespace $NS2"
cd "$ROOT"

########################################################################
section "## Lab 3 : Job and CronJob"
########################################################################
NS3=lab-job-cronjob
run "minikube image pull python:3.13-slim"
mkdir -p "$WORK/$NS3" && cd "$WORK/$NS3"
run "kubectl create namespace $NS3"

section "### 1. Creating a Simple Job"
cat > job-simple.yaml <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: data-import-job
  namespace: lab-job-cronjob
spec:
  template:
    spec:
      containers:
      - name: import
        image: python:3.13-slim
        command: ["python", "-c"]
        args:
          - |
            import time
            print("Starting data import...")
            time.sleep(3)
            print("Processing 1000 records...")
            print("Import completed!")
      restartPolicy: Never
EOF
run "kubectl apply -f job-simple.yaml"
run "kubectl -n $NS3 get job"
run "kubectl -n $NS3 wait --for=condition=complete job/data-import-job --timeout=60s"
run "kubectl -n $NS3 logs -l job-name=data-import-job"

section "### Job with retry logic"
write_retry_job() {
cat > job-with-retry.yaml <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: data-transform-job
  namespace: lab-job-cronjob
spec:
  backoffLimit: 3
  activeDeadlineSeconds: $1
  template:
    spec:
      containers:
      - name: transform
        image: python:3.13-slim
        command: ["python", "-c"]
        args:
          - |
            import sys
            print("Transforming records...")
            print("ERROR: Validation failed!")
            sys.exit(1)
      restartPolicy: Never
EOF
}
write_retry_job 300
run "kubectl apply -f job-with-retry.yaml"
run "kubectl -n $NS3 wait --for=condition=failed job/data-transform-job --timeout=240s"
run "kubectl -n $NS3 get pods -l job-name=data-transform-job"
run "kubectl -n $NS3 describe job data-transform-job"
md "**Question :** combien de pods ont été créés ? Pourquoi sont-ils conservés après l'échec ?"
answer
md "**Question :** quelle est la raison de l'échec dans la sortie \`describe\` ?"
answer
run "kubectl -n $NS3 delete job data-transform-job"
write_retry_job 10
run "grep activeDeadlineSeconds job-with-retry.yaml"
run "kubectl apply -f job-with-retry.yaml"
run "kubectl -n $NS3 wait --for=condition=failed job/data-transform-job --timeout=60s"
run "kubectl -n $NS3 describe job data-transform-job"
md "**Question :** avec \`activeDeadlineSeconds: 10\`, quelle est la nouvelle raison de l'échec ?"
answer

section "### 2. Job Parallelization"
cat > job-parallel.yaml <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: batch-export-job
  namespace: lab-job-cronjob
spec:
  parallelism: 4
  completions: 4
  completionMode: Indexed
  backoffLimit: 2
  ttlSecondsAfterFinished: 600
  template:
    spec:
      containers:
      - name: export
        image: python:3.13-slim
        command: ["python", "-c"]
        args:
          - |
            import os, time
            pod = os.getenv('HOSTNAME', 'unknown')
            shard = int(os.environ['JOB_COMPLETION_INDEX'])
            print(f"Pod {pod}: Exporting shard {shard} (users with id % 4 == {shard})...")
            time.sleep(5)
            print(f"Pod {pod}: Shard {shard} export completed!")
      restartPolicy: Never
EOF
run "kubectl apply -f job-parallel.yaml"
sleep 3
run "kubectl -n $NS3 get pods -l job-name=batch-export-job -o wide"
run "kubectl -n $NS3 wait --for=condition=complete job/batch-export-job --timeout=90s"
run "kubectl -n $NS3 get pods -l job-name=batch-export-job"
run "kubectl -n $NS3 logs -l job-name=batch-export-job --prefix"

section "### 3. Creating a CronJob"
cat > cronjob-pipeline.yaml <<'EOF'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: bronze-to-silver-pipeline
  namespace: lab-job-cronjob
spec:
  schedule: "*/2 * * * *"
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 1
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: transform
            image: python:3.13-slim
            command: ["python", "-c"]
            args:
              - |
                from datetime import datetime
                print(f"[{datetime.now()}] Bronze→Silver ETL started...")
                import time; time.sleep(2)
                print(f"[{datetime.now()}] Completed! 500 records processed")
          restartPolicy: Never
EOF
run "kubectl apply -f cronjob-pipeline.yaml"
run "kubectl -n $NS3 get cronjob"
echo "Attente de 2 exécutions du CronJob (~4 min 30)..."; sleep 270
run "kubectl -n $NS3 get jobs"
CJ_JOB=$(kubectl -n $NS3 get jobs -o name | grep bronze-to-silver-pipeline | head -1)
run "kubectl -n $NS3 logs $CJ_JOB"
cat > cronjob-backup.yaml <<'EOF'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: daily-backup
  namespace: lab-job-cronjob
spec:
  schedule: "0 2 * * *"
  timeZone: Europe/Paris
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: backup
            image: python:3.13-slim
            command: ["python", "-c", "print('Backup completed')"]
          restartPolicy: Never
EOF
run "kubectl apply -f cronjob-backup.yaml"
run "kubectl -n $NS3 get cronjob"

section "### 4. Monitoring Jobs & CronJobs"
run "kubectl -n $NS3 get jobs"
run "kubectl -n $NS3 describe job data-import-job"
run "kubectl -n $NS3 logs -l job-name=data-import-job"
run "kubectl -n $NS3 patch cronjob bronze-to-silver-pipeline -p '{\"spec\":{\"suspend\":true}}'"
run "kubectl -n $NS3 get cronjob bronze-to-silver-pipeline"
run "kubectl -n $NS3 patch cronjob bronze-to-silver-pipeline -p '{\"spec\":{\"suspend\":false}}'"
run "kubectl -n $NS3 get cronjob bronze-to-silver-pipeline"

section "### 5. Data Pipeline Scenario"
run "kubectl -n $NS3 create configmap etl-config --from-literal=BATCH_SIZE=1000 --from-literal=SOURCE_LAYER=bronze --from-literal=TARGET_LAYER=silver"
cat > cronjob-etl.yaml <<'EOF'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: etl-bronze-to-silver
  namespace: lab-job-cronjob
spec:
  schedule: "*/5 * * * *"
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      backoffLimit: 2
      ttlSecondsAfterFinished: 3600
      template:
        spec:
          containers:
          - name: etl
            image: python:3.13-slim
            envFrom:
            - configMapRef:
                name: etl-config
            resources:
              requests:
                cpu: 100m
                memory: 64Mi
              limits:
                memory: 128Mi
            command: ["python", "-c"]
            args:
              - |
                from datetime import datetime
                import os
                timestamp = datetime.now().isoformat()
                source, target = os.environ['SOURCE_LAYER'], os.environ['TARGET_LAYER']
                print(f"[{timestamp}] ETL: {source} -> {target}")
                print(f"[{timestamp}] Batch size: {os.environ['BATCH_SIZE']}")
                print(f"[{timestamp}] Reading, validating, deduplicating...")
                print(f"[{timestamp}] Writing to {target} layer...")
                print(f"[{timestamp}] Completed!")
          restartPolicy: Never
EOF
run "kubectl apply -f cronjob-etl.yaml"
run "kubectl get cronjob -n $NS3"
run "kubectl -n $NS3 create job etl-manual-run --from=cronjob/etl-bronze-to-silver"
run "kubectl -n $NS3 wait --for=condition=complete job/etl-manual-run --timeout=60s"
run "kubectl -n $NS3 logs job/etl-manual-run"

section "### Teardown"
run "kubectl -n $NS3 delete job --all"
run "kubectl -n $NS3 delete cronjob --all"
run "kubectl delete namespace $NS3"
run "kubectl get namespace $NS3"
cd "$ROOT"
run "minikube stop"

echo
echo "Terminé. Rendu généré : $OUT"
echo "Reste à faire : remplir les blocs '> **Réponse :** _à rédiger_' et l'en-tête groupe."
