package proto

import api "github.com/yyyoichi/proglog/api/v1"

func Marshal(record *api.Record) ([]byte, error) {
	return record.Value, nil
}

func Unmarshal(value []byte, record *api.Record) error {
	record.Value = value
	return nil
}
