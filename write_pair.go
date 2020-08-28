package bbolt

type WritePair struct {
	key   []byte
	value []byte
}

func WritablePair(key, value []byte) WritePair {
	return WritePair{
		key:   cloneBytes(key),
		value: cloneBytes(value),
	}
}
