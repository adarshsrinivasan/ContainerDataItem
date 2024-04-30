
ARCH := arm64

build-proto:
	python3 \
	-m grpc_tools.protoc \
	-I library/proto/ \
	--python_out=srvs/controller/rpc_api/ \
	--grpc_python_out=srvs/controller/rpc_api/ \
	--python_out=srvs/minion/rpc_api/ \
    --grpc_python_out=srvs/minion/rpc_api/ \
	--python_out=srvs/app_process/rpc_api/ \
    --grpc_python_out=srvs/app_process/rpc_api/ \
	controller-api.proto \
	&& \
	python3 \
	-m grpc_tools.protoc \
	-I library/proto/ \
	--python_out=srvs/minion/rpc_api/ \
	--grpc_python_out=srvs/minion/rpc_api/ \
	--python_out=srvs/controller/rpc_api/ \
    --grpc_python_out=srvs/controller/rpc_api/ \
	minion-api.proto \
	&& \
	python3 \
	-m grpc_tools.protoc \
	-I library/proto/ \
	--python_out=srvs/controller/rpc_api/ \
	--grpc_python_out=srvs/controller/rpc_api/ \
	--python_out=srvs/app_process/rpc_api/ \
	--grpc_python_out=srvs/app_process/rpc_api/ \
	process-api.proto


build-shm-c-lib:
	gcc -shared -fpic -o shm_lib_$(shell uname -s | tr A-Z a-z).so library/shm/shm_lib.c
	chmod 777 shm_lib_$(shell uname -s | tr A-Z a-z).so


build-common:
	$(eval SERVICE := common)
	$(eval IMAGE := adarshzededa/cdi-${SERVICE}:latest)
	docker build \
		--file srvs/${SERVICE}/Dockerfile \
		--build-arg service=${SERVICE} \
		-t ${IMAGE} \
		--platform linux/$(ARCH) . \
	&& \
	docker push ${IMAGE}

build-controller: build-common
	$(eval SERVICE := controller)
	$(eval IMAGE := adarshzededa/cdi-${SERVICE}:latest)
	docker build \
		--file srvs/${SERVICE}/Dockerfile \
		--build-arg service=${SERVICE} \
		-t ${IMAGE} \
		--platform linux/$(ARCH) . \
	&& \
	docker push ${IMAGE}

build-minion:
	$(eval SERVICE := minion)
	$(eval IMAGE := adarshzededa/cdi-${SERVICE}:latest)
	docker build \
		--file srvs/${SERVICE}/Dockerfile \
		--build-arg service=${SERVICE} \
		-t ${IMAGE} \
		--platform linux/$(ARCH) . \
	&& \
	docker push ${IMAGE}





run-controller: build-controller
	docker compose -f deployment/docker/docker-compose.yaml up -d postgres pgbouncer
	docker compose -f deployment/docker/docker-compose.yaml up controller

run-minion: build-minion run-controller
	docker compose -f deployment/docker/docker-compose.yaml up -d minion


