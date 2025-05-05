# Configuration variables
GO_SRC_DIR = ./
GO_BINARY = crdt-jepsen
GO_BINARY_PATH = $(PWD)/bin/$(GO_BINARY)
JEPSEN_DIR = ./maelstrom
NODE_COUNT = 2
TIME_LIMIT = 10
RATE = 500

# Default target
.PHONY: all
all: build test-jepsen-orset

# Build the Go binary
.PHONY: build
build:
	@echo "Building Go binary..."
	@mkdir -p bin
	go build -o $(GO_BINARY_PATH) $(GO_SRC_DIR)
	@echo "Build completed: $(GO_BINARY_PATH)"

# Run Jepsen tests
.PHONY: test-jepsen-gset
test-jepsen-gset: build
	@echo "Running Jepsen tests..."
	cd $(JEPSEN_DIR) && lein run test -w g-set --bin $(GO_BINARY_PATH) --node-count $(NODE_COUNT) --time-limit $(TIME_LIMIT) --rate $(RATE) --nemesis partition

# Run Jepsen tests
.PHONY: test-jepsen-orset
test-jepsen-orset: build
	@echo "Running Jepsen tests..."
	cd $(JEPSEN_DIR) && lein run test -w or-set --bin $(GO_BINARY_PATH) --node-count $(NODE_COUNT) --time-limit $(TIME_LIMIT) --rate $(RATE)

# Run Jepsen tests
.PHONY: test-jepsen-orset-repeat
test-jepsen-orset-repeat: build
	@echo "Running Jepsen tests..."
	cd $(JEPSEN_DIR) && lein run test -w or-set-repeat --bin $(GO_BINARY_PATH) --node-count $(NODE_COUNT) --time-limit $(TIME_LIMIT) --rate $(RATE)

# Run Jepsen tests - passing in time limit and other stuff does not work for now
.PHONY: test-jepsen-orset-perf
test-jepsen-orset-perf: build
	@echo "Running Jepsen tests..."
	cd $(JEPSEN_DIR) && lein run test -w or-set-perf --bin $(GO_BINARY_PATH) --node-count $(NODE_COUNT) --time-limit $(TIME_LIMIT) --rate $(RATE)


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
	@echo "  test-jepsen-orset : Run Jepsen tests with the built binary"
	@echo "  clean       : Remove build artifacts"
	@echo "  help        : Display this help message"
