# Tutorial: https://www.youtube.com/watch?v=zj6r_EEhv6s
deploy:
	kubectl apply -f macvlan-conf.yaml
	kubectl apply -f server.yaml
	kubectl apply -f client.yaml
delete:
	kubectl delete -f server.yaml
	kubectl delete -f client.yaml