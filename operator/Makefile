CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.17.3
ENVTEST ?= go run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.20
ENVTEST_K8S_VERSION ?= 1.32.x
IMG ?= ghcr.io/pbonard/cassandra-room-operator:latest

.PHONY: generate manifests test build docker-build

generate: ## deepcopy code
	$(CONTROLLER_GEN) object:headerFile=hack/boilerplate.go.txt paths=./api/...

manifests: ## CRDs and RBAC
	$(CONTROLLER_GEN) crd rbac:roleName=cassandra-room-operator paths=./... output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac
	cp config/crd/bases/*.yaml charts/cassandra-room-operator/crds/

test: ## unit and envtest tests
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" go test -race ./...

build:
	go build -o bin/cassandra-room-operator ./cmd

docker-build:
	docker build -t $(IMG) .
