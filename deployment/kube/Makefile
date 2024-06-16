deploy:
	cd redis && make deploy
#	cd prometheus && make deploy
	cd postgres && make deploy
	cd controller && make deploy
	cd minion && make deploy
	cd common_config && make deploy
	cd extractor && make deploy
	cd detector && make deploy
	cd combiner && make deploy
#	cd test-pod && make deploy

delete:
#	cd test-pod && make delete || true
	cd combiner && make delete || true
	cd detector && make delete || true
	cd extractor && make delete || true
	cd common_config && make delete || true
	cd minion && make delete || true
	cd controller && make delete || true
	cd postgres && make delete || true
#	cd prometheus && make delete || true
	cd redis && make delete || true

deploy-once:
	cd k8s-rdma-shared-dev-plugin && kubectl apply -k . || true
	cd multus-cni && kubectl apply -f . || true
	cd whereabouts && kubectl apply -f . || true