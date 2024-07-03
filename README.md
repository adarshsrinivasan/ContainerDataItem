# ContainerDataItem

Instructions:
1. Create a k8s cluster on CloudLab using this profile https://www.cloudlab.us/show-profile.php?uuid=d3e8c9f2-2bb6-11ef-9f39-e4434b2381fc
2. Deployment make file is located in /deployment/kube
3. Execute make deploy deploy-init, make deploy delete-cdi-infra and wait for the pods to stabilize
4. Then execute make deploy-orch, to deploy the orchestrator infrastructure
5. We can interact with the orchestrator by running srvs/testpod/client.py file. Make sure you do necessary port forwarding by following the /deployment/kube/workflow_controller/controller-ingress
6. Run the client.py, first register the tasks by giving command 3, next workflow by giving command 1, next start the workflow by giving command 5
7. obeserve the Scheduler and worker logs.
8. Also, log into the minions that exisit on the same nodes as scheduler,worker and observer logs
9. The workers are willingly deployed on different nodes to use RDMA instead of shard memmory.
10. Repeat the execution multiple times.. you will see at some point the source minion goes does with segmentation fault. 
