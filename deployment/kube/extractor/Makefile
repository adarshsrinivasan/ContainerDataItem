# Tutorial: https://www.youtube.com/watch?v=zj6r_EEhv6s
deploy:
	kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.3.0/deploy/static/provider/cloud/deploy.yaml
	kubectl apply -f extractor-deployment.yaml
	kubectl apply -f extractor-service.yaml
	sleep 60
	kubectl apply -f extractor-ingress.yaml

delete:
	kubectl delete -f extractor-ingress.yaml
	kubectl delete -f extractor-service.yaml
	kubectl delete -f extractor-deployment.yaml

forward:
	sleep 10
	nohup kubectl port-forward -n ingress-nginx service/ingress-nginx-controller 3000:80 --address 0.0.0.0 > /dev/null 2>&1 &
	# curl -i -H "Host: cdi" "http://0.0.0.0:3000/extractor/api/v1/health"