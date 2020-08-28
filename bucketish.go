package bbolt

type Bucketish interface {
	// common to Tx and Bucket
	Bucket(name []byte) *Bucket
	CreateBucket(key []byte) (*Bucket, error)
	CreateBucketIfNotExists(key []byte) (*Bucket, error)
	Cursor() *Cursor
	ForEachBucket(fn func(name []byte, b *Bucket) error) error
	DeleteBucket(key []byte) error
	Writable() bool
	ForEach(fn func(k, v []byte) error) error
}
