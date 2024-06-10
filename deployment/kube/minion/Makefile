# Tutorial: https://www.youtube.com/watch?v=zj6r_EEhv6s
deploy:
	kubectl apply -f macvlan-conf.yaml
	kubectl apply -f minion-daemonset.yaml
	kubectl apply -f minion-service.yaml

delete:
	kubectl delete -f minion-service.yaml
	kubectl delete -f minion-daemonset.yaml

forward:
	sleep 10
	nohup kubectl port-forward service/minion 50000:50000 --address 0.0.0.0 > /dev/null 2>&1 &