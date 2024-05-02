# Tutorial: https://www.youtube.com/watch?v=zj6r_EEhv6s
deploy:
	kubectl apply -f combiner-deployment.yaml
	kubectl apply -f combiner-service.yaml

delete:
	kubectl delete -f combiner-service.yaml
	kubectl delete -f combiner-deployment.yaml

forward:
	sleep 10
	nohup kubectl port-forward service/combiner-service --address 0.0.0.0  50000:50000 > /dev/null 2>&1 &