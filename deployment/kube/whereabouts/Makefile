deploy:
	kubectl apply -f whereabouts.cni.cncf.io_ippools.yaml
	kubectl apply -f whereabouts.cni.cncf.io_overlappingrangeipreservations.yaml
	kubectl apply -f daemonset-install.yaml

delete:
	kubectl delete -f daemonset-install.yaml
	kubectl delete -f whereabouts.cni.cncf.io_overlappingrangeipreservations.yaml
	kubectl delete -f whereabouts.cni.cncf.io_ippools.yaml