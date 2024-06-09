# Tutorial: https://www.youtube.com/watch?v=zj6r_EEhv6s
deploy:
	kubectl apply -f redis-configmap.yaml
	kubectl apply -f redis-statefulset.yaml
	kubectl apply -f redis-service.yaml

delete:
	kubectl delete -f redis-service.yaml
	kubectl delete -f redis-statefulset.yaml
	kubectl delete -f redis-configmap.yaml
	kubectl delete pvc -l app=redis

forward:
	sleep 10
	nohup kubectl port-forward service/redis --address 0.0.0.0  6379:6379 > /dev/null 2>&1 &