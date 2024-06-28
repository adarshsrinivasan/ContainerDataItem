# Tutorial: https://www.youtube.com/watch?v=zj6r_EEhv6s
deploy:
	kubectl apply -f deployment.yaml
	kubectl apply -f service.yaml
	kubectl apply -f controller-ingress.yaml

delete:
	kubectl delete -f controller-ingress.yaml
	kubectl delete -f deployment.yaml
	kubectl delete -f service.yaml

forward:
	sleep 10
	nohup kubectl port-forward -n ingress-nginx service/ingress-nginx-controller 3000:80 --address 0.0.0.0 > /dev/null 2>&1 &
