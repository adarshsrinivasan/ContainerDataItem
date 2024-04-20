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
    --python_out=srvs/extractor/rpc_api/ \
    --python_out=srvs/object_detector/rpc_api/ \
    --python_out=srvs/combiner/rpc_api/ \
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
	--python_out=srvs/combiner/rpc_api/ \
    --grpc_python_out=srvs/combiner/rpc_api/ \
    --python_out=srvs/extractor/rpc_api/ \
    --grpc_python_out=srvs/extractor/rpc_api/ \
    --python_out=srvs/object_detector/rpc_api/ \
    --grpc_python_out=srvs/object_detector/rpc_api/ \
	process-api.proto


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

build-extractor:
	$(eval SERVICE := extractor)
	$(eval IMAGE := adarshzededa/cdi-${SERVICE}-rpc:latest)
	docker build \
		--file srvs/${SERVICE}/Dockerfile \
		--build-arg service=${SERVICE} \
		-t ${IMAGE} \
		--platform linux/$(ARCH) . \
	&& \
	docker push ${IMAGE}

build-detector:
	$(eval SERVICE := detector)
	$(eval IMAGE := adarshzededa/cdi-${SERVICE}-rpc:latest)
	docker build \
		--file srvs/${SERVICE}/Dockerfile \
		--build-arg service=${SERVICE} \
		-t ${IMAGE} \
		--platform linux/$(ARCH) . \
	&& \
	docker push ${IMAGE}

build-combiner:
	$(eval SERVICE := combiner)
	$(eval IMAGE := adarshzededa/cdi-${SERVICE}-rpc:latest)
	docker build \
		--file srvs/${SERVICE}/Dockerfile \
		--build-arg service=${SERVICE} \
		-t ${IMAGE} \
		--platform linux/$(ARCH) . \
	&& \
	docker push ${IMAGE}



