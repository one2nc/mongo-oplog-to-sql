package domain

type OplogEntry struct {
	Operation string                 `json:"op"`
	Namespace string                 `json:"ns"`
	Object    map[string]interface{} `json:"o"`
	Object2   map[string]interface{} `json:"o2"`
}
