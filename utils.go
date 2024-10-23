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
