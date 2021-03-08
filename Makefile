.DEFAULT_GOAL := build-all

export GO15VENDOREXPERIMENT=1

build-all: pika-manager-dashboard pika-manager-admin clean-gotest

pika-manager-deps:
	@mkdir -p bin config && bash version
pika-manager-dashboard: pika-manager-deps
	go build -i -o bin/pika-manager ./cmd/dashboard
	@./bin/pika-manager --default-config > config/pika-manager.toml

pika-mannager-admin: pika-manager-deps
	go build -i -o bin/pika-manager-admin ./cmd/admin

clean-gotest:
	@rm -rf ./pkg/topom/gotest.tmp

clean: clean-gotest
	@rm -rf bin
	@rm -rf scripts/tmp

gotest: pika-manager-deps
	go test ./cmd/... ./pkg/...

gobench: pika-manager-deps
	go test -gcflags -l -bench=. -v ./pkg/...

docker:
	docker build --force-rm -t pika-manager-image .

demo:
	pushd example && make
