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
	StandaloneSize() uint64

	// only on Bucket
	Delete(key []byte) error
	Get(key []byte) []byte
	MultiGet(pairs ...[]byte) (values [][]byte, err error)
	MultiPut(pairs ...[]byte) error
	NextSequence() (uint64, error)
	Put(key []byte, value []byte) error
	Sequence() uint64
	SetSequence(v uint64) error
}

// Bucket implements Bucketish directly

// RootBucket is an implementation of Bucketish for Tx

type RootBucket struct {
	tx *Tx
}

func NewRootBucket(tx *Tx) *RootBucket {
	return &RootBucket{
		tx: tx,
	}
}

// --- methods implemented on Tx
//

func (b *RootBucket) Bucket(name []byte) *Bucket {
	return b.tx.Bucket(name)
}

func (b *RootBucket) CreateBucket(key []byte) (*Bucket, error) {
	return b.tx.CreateBucket(key)
}

func (b *RootBucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	return b.tx.CreateBucketIfNotExists(key)
}

func (b *RootBucket) Cursor() *Cursor {
	return b.tx.Cursor()
}

func (b *RootBucket) ForEachBucket(fn func(name []byte, b *Bucket) error) error {
	return b.tx.ForEachBucket(fn)
}

func (b *RootBucket) DeleteBucket(key []byte) error {
	return b.tx.DeleteBucket(key)
}

func (b *RootBucket) Writable() bool {
	return b.tx.Writable()
}

func (b *RootBucket) ForEach(fn func(k, v []byte) error) error {
	return b.tx.ForEach(fn)
}

func (b *RootBucket) StandaloneSize() uint64 {
	return b.tx.root.StandaloneSize()
}

// --- methods NOT implemented on Tx
//

func (b *RootBucket) Delete(key []byte) error {
	return ErrIncompatibleValue
}

func (b *RootBucket) Get(key []byte) []byte {
	// return ErrIncompatibleValue
	return nil
}

func (b *RootBucket) MultiGet(pairs ...[]byte) (values [][]byte, err error) {
	return nil, ErrIncompatibleValue
}

func (b *RootBucket) MultiPut(pairs ...[]byte) error {
	return ErrIncompatibleValue
}

func (b *RootBucket) NextSequence() (uint64, error) {
	return 0, ErrIncompatibleValue
}

func (b *RootBucket) Put(key []byte, value []byte) error {
	return ErrIncompatibleValue
}

func (b *RootBucket) Sequence() uint64 {
	// return ErrIncompatibleValue
	return 0
}

func (b *RootBucket) SetSequence(v uint64) error {
	return ErrIncompatibleValue
}
