# Tutorial: https://www.youtube.com/watch?v=zj6r_EEhv6s
deploy:
	kubectl apply -f detector-deployment.yaml
	kubectl apply -f detector-service.yaml

delete:
	kubectl delete -f detector-service.yaml
	kubectl delete -f detector-deployment.yaml

forward:
	sleep 10
	nohup kubectl port-forward service/detector-service --address 0.0.0.0  50000:50000 > /dev/null 2>&1 &