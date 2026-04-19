BINARY     := scp
PLUGIN_DIR := $(HOME)/.docker/cli-plugins
PLUGIN     := $(PLUGIN_DIR)/docker-scp

.PHONY: build
build:
	go build -o $(BINARY) .

.PHONY: install-docker-plugin
install-docker-plugin: build
	mkdir -p $(PLUGIN_DIR)
	cp $(BINARY) $(PLUGIN)

.PHONY: uninstall-docker-plugin
uninstall-docker-plugin:
	rm -f $(PLUGIN)

# Tools used by fmt/lint. Install with:
#   go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest

.PHONY: fmt
fmt:
	golangci-lint fmt ./...

.PHONY: lint
lint:
	golangci-lint run ./...

.PHONY: clean
clean:
	rm -f $(BINARY)
