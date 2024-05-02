# Tutorial: https://www.youtube.com/watch?v=zj6r_EEhv6s
deploy:
	kubectl apply -f controller-deployment.yaml
	kubectl apply -f controller-service.yaml

delete:
	kubectl delete -f controller-service.yaml
	kubectl delete -f controller-deployment.yaml

forward:
	sleep 10
	nohup kubectl port-forward service/controller 50000:50000 --address 0.0.0.0 > /dev/null 2>&1 &