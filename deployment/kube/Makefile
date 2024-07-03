deploy: deploy-cdi-infra deploy-orch
delete: delete-orch delete-cdi-infra
deploy-all: deploy-init deploy-cdi-infra deploy-orch
delete-all: delete-init delete-cdi-infra delete-orch

deploy-orch:
	cd workflow_controller && make deploy
	cd workflow_scheduler && make deploy
	cd workflow_worker && make deploy

delete-orch:
	cd workflow_controller && make delete || true
	cd workflow_scheduler && make delete || true
	cd workflow_worker && make delete || true

deploy-cdi-infra:
	cd redis && make deploy
	cd postgres && make deploy
	cd controller && make deploy
	cd minion && make deploy
	cd common_config && make deploy

deploy-rdma-tests:
	cd k8s-rdma-shared-dev-plugin && make deploy
	cd multus-cni && make deploy
	cd whereabouts && make deploy
	cd rdma && make deploy

delete-cdi-infra:
	cd common_config && make delete || true
	cd minion && make delete || true
	cd controller && make delete || true
	cd postgres && make delete || true
	cd redis && make delete || true


deploy-init:
	kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.3.0/deploy/static/provider/cloud/deploy.yaml
	cd k8s-rdma-shared-dev-plugin && make deploy
	cd multus-cni && make deploy
	cd whereabouts && make deploy
	cd test-pod && make deploy
	cd prometheus && make deploy

delete-init:
	cd prometheus && make delete || true
	cd test-pod && make delete || true
	cd whereabouts && make delete || true
	cd multus-cni && make delete || true
	cd k8s-rdma-shared-dev-plugin && make delete || true
	kubectl delete -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.3.0/deploy/static/provider/cloud/deploy.yaml