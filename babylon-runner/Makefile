BINARY     := babylon-runner
BUILD_DIR  := bin
GOFLAGS    := -trimpath
LDFLAGS    := -s -w

.PHONY: build run test lint clean docker-build fmt vet

build:
	CGO_ENABLED=0 go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BUILD_DIR)/$(BINARY) ./cmd/babylon-runner/

run: build
	$(BUILD_DIR)/$(BINARY)

test:
	go test ./...

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

clean:
	rm -rf $(BUILD_DIR)

docker-build:
	podman build -t $(BINARY):latest .
