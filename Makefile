# Configuration variables
GO_SRC_DIR = ./
GO_BINARY = crdt-jepsen
GO_BINARY_PATH = $(PWD)/bin/$(GO_BINARY)
JEPSEN_DIR = ../maelstrom

# Default target
.PHONY: all
all: build test-jepsen

# Build the Go binary
.PHONY: build
build:
	@echo "Building Go binary..."
	@mkdir -p bin
	go build -o $(GO_BINARY_PATH) $(GO_SRC_DIR)
	@echo "Build completed: $(GO_BINARY_PATH)"

# Run Jepsen tests
.PHONY: test-jepsen
test-jepsen: build
	@echo "Running Jepsen tests..."
	cd $(JEPSEN_DIR) && lein run test -w or-set --bin $(GO_BINARY_PATH) --node-count 3 --time-limit 15 --rate 5

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin/
	@echo "Clean completed"

# Help target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all         : Build the Go project and run Jepsen tests (default)"
	@echo "  build       : Build the Go binary only"
	@echo "  test-jepsen : Run Jepsen tests with the built binary"
	@echo "  clean       : Remove build artifacts"
	@echo "  help        : Display this help message"