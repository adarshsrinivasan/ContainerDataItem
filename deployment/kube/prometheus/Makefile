# Referred from:
# https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack#kube-prometheus-stack
# https://www.civo.com/learn/kubernetes-node-monitoring-with-prometheus-and-grafana

deploy:
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo update
	helm install cdi-monitor prometheus-community/kube-prometheus-stack
	# kubectl --namespace default get pods -l "release=cdi-monitor"

delete:
	helm uninstall cdi-monitor
	kubectl delete crd alertmanagerconfigs.monitoring.coreos.com
	kubectl delete crd alertmanagers.monitoring.coreos.com
	kubectl delete crd podmonitors.monitoring.coreos.com
	kubectl delete crd probes.monitoring.coreos.com
	kubectl delete crd prometheusagents.monitoring.coreos.com
	kubectl delete crd prometheuses.monitoring.coreos.com
	kubectl delete crd prometheusrules.monitoring.coreos.com
	kubectl delete crd scrapeconfigs.monitoring.coreos.com
	kubectl delete crd servicemonitors.monitoring.coreos.com
	kubectl delete crd thanosrulers.monitoring.coreos.com

forward:
	sleep 10
	#username: admin
	#password - get from: kubectl get secret cdi-monitor-grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo
	nohup kubectl port-forward service/cdi-monitor-grafana --address 0.0.0.0  3000:80 > /dev/null 2>&1 &