package main

func copyMap(original map[string]int) map[string]int {
	newMap := make(map[string]int)
	for key, value := range original {
		newMap[key] = value
	}
	return newMap
}
func copyMapOfMaps(original map[string]map[string]int) map[string]map[string]int {
	newMap := make(map[string]map[string]int)
	for key, value := range original {
		newMap[key] = copyMap(value)
	}
	return newMap
}

// Function to deep copy a slice of events
func deepCopyEvents(original []event) []event {
	// Create a new slice with the same length as the original
	copied := make([]event, len(original))

	// Iterate over each event in the original slice
	for i, v := range original {
		// Shallow copy the event struct fields
		copied[i] = v

		// Create a new map for vectorClockBefore to ensure deep copy
		copied[i].vector_clock_before = make(map[string]int)
		for key, value := range v.vector_clock_before {
			copied[i].vector_clock_before[key] = value
		}
	}

	return copied
}
