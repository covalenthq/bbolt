package bbolt

type Location struct {
  parent Bucketish
  childKey []byte
}

func NewLocation(parent Bucketish, childKey []byte) *Location {
  return &Location{
    parent: parent,
    childKey: childKey,
  }
}

func (loc *Location) Parent() Bucketish {
  return loc.parent
}

func (loc *Location) Key() []byte {
  return loc.childKey
}

func (loc *Location) ResolveHere() interface{} {
  if v := loc.GetHere(); v != nil {
    return v
  } else if b := loc.BucketHere(); b != nil {
    return b
  } else if rb := loc.RootBucketHere(); rb != nil {
    return rb
  } else {
    return nil
  }
}

func (loc *Location) GetHere() []byte {
  if loc.childKey != nil {
    return loc.parent.Get(loc.childKey)
  }

  return nil
}

func (loc *Location) PutHere(value []byte) error {
  if loc.childKey != nil {
    return loc.parent.Put(loc.childKey, value)
  }

  return ErrIncompatibleValue
}

func (loc *Location) DeleteHere() error {
  if loc.childKey != nil {
    return loc.parent.Delete(loc.childKey)
  }

  return ErrIncompatibleValue
}

func (loc *Location) BucketishHere() Bucketish {
  if loc.childKey != nil {
    return loc.parent.Bucket(loc.childKey)
  }

  return loc.parent
}

func (loc *Location) BucketHere() *Bucket {
  if loc.childKey != nil {
    return loc.parent.Bucket(loc.childKey)
  }

  if b, ok := loc.parent.(*Bucket); ok && b != nil {
    return b
  }

  return nil
}

func (loc *Location) RootBucketHere() *RootBucket {
  if loc.childKey != nil {
    return nil
  }

  if rb, ok := loc.parent.(*RootBucket); ok && rb != nil {
    return rb
  }

  return nil
}

func (loc *Location) CreateBucketHere() (*Bucket, error) {
  if loc.childKey != nil {
    return loc.parent.CreateBucket(loc.childKey)
  }

  if _, ok := loc.parent.(*Bucket); ok {
    return nil, ErrBucketExists
  }

  return nil, ErrIncompatibleValue
}

func (loc *Location) CreateBucketHereIfNotExists() (*Bucket, error) {
  if loc.childKey != nil {
    return loc.parent.CreateBucketIfNotExists(loc.childKey)
  }

  if b, ok := loc.parent.(*Bucket); ok {
    return b, nil
  }

  return nil, ErrIncompatibleValue
}

func (loc *Location) DeleteBucketHere() error {
  if loc.childKey != nil {
    return loc.parent.DeleteBucket(loc.childKey)
  }

  return ErrIncompatibleValue
}

func (loc *Location) Writable() bool {
  return loc.parent.Writable()
}
