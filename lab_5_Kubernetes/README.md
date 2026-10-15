# Lab 5 : Kubernetes (06.containerization-and-kubernetes)

- `lab-1.md`, `lab-2.md`, `lab-3.md`: lab instructions
- `deployment.yaml`: Deployment `kubernetes-bootcamp` completed (image `kubernetes-bootcamp:v1`, port 8080, 3 replicas)
- `service.yaml`: NodePort Service completed (selector `app: kubernetes-bootcamp`, port 8080)
- `run_tp.sh`: runs labs 1, 2 and 3 on minikube and writes every command and its output to `RENDU_TP_K8S.md`
- `RENDU_TP_K8S.md`: commands, outputs and answers to the lab questions

## Run

Requirements: Docker, minikube, kubectl, jq, curl.

```bash
cd lab_5_Kubernetes
./run_tp.sh
```

AI usage: an AI assistant was used to help write `run_tp.sh` and complete the manifests.
