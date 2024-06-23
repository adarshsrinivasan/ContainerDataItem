deploy:
	cd redis && make deploy
	cd postgres && make deploy
	cd controller && make deploy
	cd minion && make deploy
	cd common_config && make deploy
	cd extractor && make deploy
	cd detector && make deploy
	cd combiner && make deploy

delete:
	cd combiner && make delete || true
	cd detector && make delete || true
	cd extractor && make delete || true
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