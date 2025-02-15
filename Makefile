VERSION ?= 0.2.2
UPSTREAM_BRANCH ?= origin/master

.PHONY: prepare
prepare:
	docker login

.PHONY: deps
deps:
	go get -v github.com/vitkovskii/insane-doc@v0.0.1

.PHONY: cover
cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out
	rm coverage.out

.PHONY: test
test:
	go test ./fd/ -v -count 1
	go test ./pipeline/ -v -count 1
	go test ./plugin/... -v -count 1

.PHONY: test-short
test-short:
	go test ./fd/ -v -count 1 -short
	go test ./pipeline/ -v -count 1 -short
	go test ./plugin/... -v -count 1 -short

.PHONY: test-e2e
test-e2e:
	go test ./cmd/ -v -count 1

.PHONY: bench-file
bench-file:
	go test -bench LightJsonReadPar ./plugin/input/file -v -count 1 -run -benchmem -benchtime 1x

.PHONY: gen-doc
gen-doc:
	go install github.com/vitkovskii/insane-doc@latest
	insane-doc

.PHONY: profile-file
profile-file:
	go test -bench LightJsonReadPar ./plugin/input/file -v -count 1 -run -benchmem -benchtime 1x -cpuprofile cpu.pprof -memprofile mem.pprof -mutexprofile mutex.pprof

.PHONY: push-version-linux-amd64
push-version-linux-amd64:
	GOOS=linux GOARCH=amd64 go build -v -o file.d ./cmd/file.d.go
	docker build -t ozonru/file.d:${VERSION}-linux-amd64 .
	docker push ozonru/file.d:${VERSION}-linux-amd64

.PHONY: push-latest-linux-amd64
push-latest-linux-amd64:
	GOOS=linux GOARCH=amd64 go build -v -o file.d ./cmd/file.d.go
	docker build -t ozonru/file.d:latest-linux-amd64 .
	docker push ozonru/file.d:latest-linux-amd64

.PHONY: push-images-version
push-images-version: prepare push-version-linux-amd64

.PHONY: push-images-latest
push-images-latest: prepare push-latest-linux-amd64

.PHONY: push-images-all
push-images-all: push-images-version push-images-latest

.PHONY: lint
lint:
	# installation: https://golangci-lint.run/usage/install/#local-installation
	golangci-lint run --new-from-rev=${UPSTREAM_BRANCH}

.PHONY: mock
mock:
	go install github.com/golang/mock/mockgen
	mockgen -source=plugin/output/s3/s3.go -destination=plugin/output/s3/mock/s3.go
