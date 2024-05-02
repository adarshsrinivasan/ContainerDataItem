
ARCH := arm64

build-proto:
	python3 \
	-m grpc_tools.protoc \
	-I library/proto/ \
    --python_out=srvs/common/rpc_api/ \
    --grpc_python_out=srvs/common/rpc_api/ \
	controller-api.proto \
	minion-api.proto \
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

build-controller:
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

build-extractor:
	$(eval SERVICE := extractor)
	$(eval IMAGE := adarshzededa/cdi-${SERVICE}:latest)
	docker build \
		--file srvs/${SERVICE}/Dockerfile \
		--build-arg service=${SERVICE} \
		-t ${IMAGE} \
		--platform linux/$(ARCH) . \
	&& \
	docker push ${IMAGE}

build-detector:
	$(eval SERVICE := detector)
	$(eval IMAGE := adarshzededa/cdi-${SERVICE}:latest)
	docker build \
		--file srvs/${SERVICE}/Dockerfile \
		--build-arg service=${SERVICE} \
		-t ${IMAGE} \
		--platform linux/$(ARCH) . \
	&& \
	docker push ${IMAGE}

build-combiner:
	$(eval SERVICE := combiner)
	$(eval IMAGE := adarshzededa/cdi-${SERVICE}:latest)
	docker build \
		--file srvs/${SERVICE}/Dockerfile \
		--build-arg service=${SERVICE} \
		-t ${IMAGE} \
		--platform linux/$(ARCH) . \
	&& \
	docker push ${IMAGE}

build-all: build-proto build-common build-controller build-minion build-extractor build-detector build-combiner


