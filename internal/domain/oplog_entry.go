package domain

import "strings"

const (
	SEPERATOR string = "."
)

type OplogEntry struct {
	Operation string                 `json:"op"`
	Namespace string                 `json:"ns"`
	Object    map[string]interface{} `json:"o"`
	Object2   map[string]interface{} `json:"o2"`
}

func (o OplogEntry) DatabaseName() string {
	return strings.ToLower(strings.Split(o.Namespace, SEPERATOR)[0])
}

func (o OplogEntry) TableName() string {
	return strings.ToLower(strings.Split(o.Namespace, SEPERATOR)[1])
}
