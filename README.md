# maelstrom-broadcast: ORSet CRDT Implementation

A Go implementation of an Observed-Remove Set (ORSet) CRDT that works with [Jepsen's Maelstrom](https://github.com/jepsen-io/maelstrom) distributed systems testing framework.

## Architecture

This implementation is structured in two distinct layers:

### 1. Causal Layer

The foundational layer handles the sequencing of events across distributed nodes, maintaining causal relationships between operations. Key components include:

- **Event Log**: Records all operations with vector clocks to establish happens-before relationships
- **Vector Clock System**: Tracks causal dependencies between events
- **Message Deduplication**: Ensures idempotent message processing with the `unique_messages` map
- **Event Synchronization**: Periodic and triggered event propagation between nodes

This layer ensures that if a remove event happens after an add event, that causal relationship is preserved across all nodes.

### 2. CRDT Layer

Built on top of the causal layer, this implements the specific semantics of an ORSet:

- **Identity-First Approach** (master branch): Maps unique operation identifiers to numeric values
- **Value-First Approach** (value-first branch): Maps values to lists of unique identifiers, which performs better for workloads with hot keys

## Implementation Analysis

### Vector Clock Usage

The implementation maintains a comprehensive vector clock system for establishing causal relationships:

1. Each event stores the vector clock at its creation time (`vector_clock_before`)
2. Nodes exchange vector clocks during synchronization to efficiently determine which events need to be propagated
3. Vector clock comparison (`compareVClock()`) ensures nodes only receive events when they have processed all causally preceding events

This approach ensures proper happens-before relationships are maintained even during complex network partition scenarios.

### ORSet Mechanics

The delete operation implementation correctly maintains references to the add operations it removes through the `add_msg_uid_for_delete` field, which is essential for proper ORSet semantics. This allows the system to determine which specific "add" instances to remove when processing a "delete" operation.

```go
// Delete handling ensures causal relationship with adds
new_event := event{msgDest, msg_uid, int(body["element"].(float64)), "delete", slice_element_add_uids, element_exists, vectorClockCopy}
```

### Synchronization Strategy

The implementation uses a hybrid push-pull mechanism:
- Events are pushed immediately when created (`new_send_event_trigger`)
- A periodic timer ensures eventual consistency even during quiet periods
- Vector clocks are exchanged to determine which events need synchronization

## Performance Characteristics

- **Operation Batching**: Events are processed in batches to reduce network overhead
- **Mutex Segmentation**: Uses multiple fine-grained locks to reduce contention
- **Efficient Deduplication**: Fast map-based message deduplication

## Branch Comparison

The repository offers two different approaches to the ORSet implementation:

- **master**: Identity-first approach - a map from a unique identifier to a numeric value
- **value-first**: Maps values to a list of unique identifiers - works better for workloads with hot keys, and even for average workloads it is better or on par with the first approach

## Potential Improvements

1. **Pluggable CRDT Layer**: Abstract the CRDT implementation to easily swap different types
2. **Performance Optimizations**: Further reduce lock contention, optimize message processing paths, and implement batched event processing

## Usage with Maelstrom

The project includes a Makefile for easy building and testing:

```bash
# Build and run the default ORSet test
make

# Build only
make build

# Run different test workloads
make test-jepsen-gset           # Test with grow-only set
make test-jepsen-orset          # Test with observed-remove set
make test-jepsen-orset-repeat   # Test with repeated elements
make test-jepsen-orset-perf     # Performance testing

# Clean build artifacts
make clean

# Display help
make help
```

You can configure test parameters (NODE_COUNT, TIME_LIMIT, RATE) by modifying the Makefile variables.