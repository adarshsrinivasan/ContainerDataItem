deploy:
	cd redis && make deploy
	cd prometheus && make deploy
	cd postgres && make deploy
	cd controller && make deploy
	cd minion && make deploy
	cd common_config && make deploy
	cd extractor && make deploy
	cd detector && make deploy
	cd combiner && make deploy
	cd test-pod && make deploy
	cd workflow_worker && make deploy
	cd workflow_scheduler && make deploy
	cd workflow_controller && make deploy

delete:
	cd test-pod && make delete || true
	cd combiner && make delete || true
	cd detector && make delete || true
	cd extractor && make delete || true
	cd common_config && make delete || true
	cd minion && make delete || true
	cd controller && make delete || true
	cd postgres && make delete || true
	cd prometheus && make delete || true
	cd redis && make delete || true
	cd workflow_worker && make delete || true
	cd workflow_scheduler && make delete || true
	cd workflow_controller && make delete || true