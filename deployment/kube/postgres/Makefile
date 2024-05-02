# Tutorial: https://www.youtube.com/watch?v=zj6r_EEhv6s
deploy:
	kubectl apply -f postgres-configmap.yaml
	kubectl apply -f postgres-statefulset.yaml
	kubectl apply -f postgres-service.yaml

delete:
	kubectl delete -f postgres-service.yaml
	kubectl delete -f postgres-statefulset.yaml
	kubectl delete -f postgres-configmap.yaml
	kubectl delete pvc -l app=postgres

forward:
	sleep 10
	nohup kubectl port-forward service/postgres --address 0.0.0.0  5432:5432 > /dev/null 2>&1 &