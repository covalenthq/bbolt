package main

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	bolt "github.com/covalenthq/bbolt"
)

var (
	// ErrUsage is returned when a usage message was printed and the process
	// should simply exit with an error.
	ErrUsage = errors.New("usage")

	// ErrUnknownCommand is returned when a CLI command is not specified.
	ErrUnknownCommand = errors.New("unknown command")

	// ErrPathRequired is returned when the path to a Bolt database is not specified.
	ErrPathRequired = errors.New("path required")

	// ErrBoltURIRequired is returned when an argument expected to be a well-formed
	// bolt://... URI is not.
	ErrBoltURIRequired = errors.New("expected <bolt://...> URI")

	// ErrAliasNotFound is returned when no Bolt database was mounted with the
	// given name.
	ErrAliasNotFound = errors.New("alias not found")

	// ErrFileNotFound is returned when a Bolt database does not exist.
	ErrFileNotFound = errors.New("file not found")

	// ErrBucketRequired is returned when a bucket is not specified.
	ErrBucketRequired = errors.New("bucket required")

	// ErrBucketNotFound is returned when a bucket is not found.
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrBucketNotEmpty is returned when a bucket targeted for deletion is not empty.
	ErrBucketNotEmpty = errors.New("bucket not empty")

	// ErrBucketIsRoot is returned when a bucket targeted for deletion is the Tx root page.
	ErrBucketIsRoot = errors.New("cowardly refusing to delete root of database")

	// ErrKeyRequired is returned when a key is not specified.
	ErrKeyRequired = errors.New("key required")

	// ErrKeyIsBucket is returned when a key expected to resolve to a scalar value
	// instead resolves to a bucket.
	ErrKeyIsBucket = errors.New("key is bucket")

	// ErrKeyNotFound is returned when a key is not found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyNotFound is returned when a key is found.
	ErrKeyFound = errors.New("key found")
)

type commandEnvironment struct {
	args []string

	inIO  io.Reader
	outIO io.Writer
	errIO io.Writer

	mounts    map[string]string
	txHandles map[string]*bolt.Tx
}

func main() {
	if err := execSubcommand(os.Args[1:]); err == ErrUsage {
		fmt.Fprintln(os.Stderr, Usage())
		os.Exit(2)
	} else if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func execSubcommand(args []string) error {
	mounts := make(map[string]string)

	for len(args) >= 2 && (args[0] == "-d" || args[0] == "--database") {
		alias_and_path := strings.SplitN(args[1], ":", 2)

		var alias, path_part string
		switch len(alias_and_path) {
		case 1:
			path_part = alias_and_path[0]
			alias = strings.TrimSuffix(path.Base(path_part), ".db")
		case 2:
			alias = alias_and_path[0]
			path_part = alias_and_path[1]
		default:
			return ErrUsage
		}

		mounts[alias] = path_part

		args = args[2:]
	}

	var subcommand string
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return ErrUsage
	}
	subcommand, args = args[0], args[1:]

	for _, dbPath := range mounts {
		if dbPath == "" {
			return ErrPathRequired
		}
	}

	cmdEnv := &commandEnvironment{
		mounts:    mounts,
		txHandles: make(map[string]*bolt.Tx),
		args:      args,
		inIO:      os.Stdin,
		outIO:     os.Stdout,
		errIO:     os.Stderr,
	}

	// Execute command.
	switch subcommand {
	case "help":
		return ErrUsage
	case "touch":
		return touchDatabaseFile(cmdEnv)
	case "get":
		return getKey(cmdEnv)
	case "put":
		return putKeyValue(cmdEnv)
	case "mkdir":
		return makeBucket(cmdEnv)
	case "rm":
		return removeKey(cmdEnv)
	case "cp":
		return copyKeyWithFile(cmdEnv)
	case "ls":
		return listKeys(cmdEnv)
	case "tree":
		return printBucketTree(cmdEnv)
	case "du":
		return diskUsage(cmdEnv)
	default:
		return ErrUnknownCommand
	}
}

// Usage returns the help message.
func Usage() string {
	return strings.TrimLeft(`
boltutil(1) is a tool for manipulating Bolt databases, with syntax similar to
Google Cloud Storage's gsutil(1).

### BOLT URI FORMAT

boltutil uses 'bolt://dbname/keypath' URIs to refer to buckets and keys
within Bolt database files.

To refer to a key within a database, it must be mounted to an alias,
using the -d or --database flag. The syntax of the flag is:

    --[d]atabase "alias:path"

The following call, for example, creates two mounts, allowing the resolution of
the URIs <bolt://foo/...> and <bolt://bar/...>:

    boltutil -d "foo:x/y/z/foo.db" -d "bar:./bar.db" [...]

As a convenience, if a dbname is not specified, the basename of the database
file, with the '.db' extension stripped, will be used as the dbname:

    # mounts <bolt://foo/...>
    boltutil --db "x/y/z/foo.db" [...]

### USAGES

  boltutil touch <bolt-alias>

  boltutil get <bolt-uri>
  boltutil put <bolt-uri> <value>

  boltutil mkdir <bolt-uri>
  boltutil rm [-r] <bolt-uri>
  boltutil cp [-r] <bolt-uri> <bolt-uri>

  boltutil ls <bolt-uri>
  boltutil tree [-d MAXDEPTH] <bolt-uri>
  boltutil du [-d MAXDEPTH] <bolt-uri>
`, "\n")
}

func resolveBoltURI(env *commandEnvironment, rawURI string, wantWritableTx bool, cb func(*bolt.Location) error) error {
	uri, err := url.Parse(rawURI)
	if err != nil {
		return err
	}
	if uri.Scheme != "bolt" {
		return ErrBoltURIRequired
	}

	mountAlias := uri.Hostname()

	if txHandle, ok := env.txHandles[mountAlias]; ok {
		if wantWritableTx && !txHandle.Writable() {
			return bolt.ErrTxNotWritable
		}

		loc, err := navigateToLocation(txHandle, uri.Path)
		if err != nil {
			return err
		}

		return cb(loc)
	}

	path, ok := env.mounts[mountAlias]
	if !ok {
		return ErrAliasNotFound
	}

	if !wantWritableTx {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return ErrFileNotFound
		}
	}

	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	task := func(txHandle *bolt.Tx) error {
		env.txHandles[mountAlias] = txHandle
		defer func() {
			delete(env.txHandles, mountAlias)
		}()

		loc, err := navigateToLocation(txHandle, uri.Path)
		if err != nil {
			return err
		}

		return cb(loc)
	}

	if wantWritableTx {
		return db.Update(task)
	} else {
		return db.View(task)
	}
}

func isBoltURI(rawURI string) bool {
	uri, err := url.Parse(rawURI)
	if err != nil {
		return false
	}
	if uri.Scheme != "bolt" {
		return false
	}

	return true
}

func slashP(c rune) bool {
	return c == '/'
}

func navigateToLocation(txHandle *bolt.Tx, path string) (*bolt.Location, error) {
	keyPath := strings.FieldsFunc(strings.Trim(path, "/"), slashP)

	var keyPathLast []byte

	if len(keyPath) > 0 {
		keyPathLast = []byte(keyPath[len(keyPath)-1])
		keyPath = keyPath[:len(keyPath)-1]
	}

	bish := bolt.Bucketish(bolt.NewRootBucket(txHandle))
	for _, childKey := range keyPath {
		bish = bish.Bucket([]byte(childKey))
		if b, ok := bish.(*bolt.Bucket); !ok || b == nil {
			return nil, ErrBucketNotFound
		}
	}

	return bolt.NewLocation(bish, keyPathLast), nil
}

func touchDatabaseFile(env *commandEnvironment) error {
	if len(env.args) != 1 {
		return ErrUsage
	}

	mountAlias := env.args[0]

	path, ok := env.mounts[mountAlias]
	if !ok {
		return ErrAliasNotFound
	}

	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	return nil
}

func getKey(env *commandEnvironment) error {
	if len(env.args) != 1 {
		return ErrUsage
	}

	return resolveBoltURI(env, env.args[0], false, func(loc *bolt.Location) error {
		something := loc.ResolveHere()

		if v, ok := something.([]byte); ok && v != nil {
			fmt.Printf("%#x\n", v)
			return nil
		} else if rb, ok := something.(*bolt.RootBucket); ok && rb != nil {
			return ErrKeyIsBucket
		} else if b, ok := something.(*bolt.Bucket); ok && b != nil {
			return ErrKeyIsBucket
		} else {
			return ErrKeyNotFound
		}
	})
}

func putKeyValue(env *commandEnvironment) error {
	if len(env.args) != 2 {
		return ErrUsage
	}

	return resolveBoltURI(env, env.args[0], true, func(loc *bolt.Location) error {
		return loc.PutHere([]byte(env.args[1]))
	})
}

func makeBucket(env *commandEnvironment) error {
	if len(env.args) != 1 {
		return ErrUsage
	}

	return resolveBoltURI(env, env.args[0], true, func(loc *bolt.Location) error {
		_, err := loc.CreateBucketHereIfNotExists()
		return err
	})
}

func removeKey(env *commandEnvironment) error {
	recurse := false
	if len(env.args) >= 1 && (env.args[0] == "-r" || env.args[0] == "--recurse") {
		recurse = true
		env.args = env.args[1:]
	}

	if len(env.args) != 1 {
		return ErrUsage
	}

	return resolveBoltURI(env, env.args[0], true, func(loc *bolt.Location) error {
		something := loc.ResolveHere()

		if rb, ok := something.(*bolt.RootBucket); ok && rb != nil {
			return ErrBucketIsRoot
		} else if b, ok := something.(*bolt.Bucket); ok && b != nil {
			if !(bucketIsEmpty(b) || recurse) {
				return ErrBucketNotEmpty
			}
			return loc.DeleteBucketHere()
		} else if v, ok := something.([]byte); ok && v != nil {
			return loc.DeleteHere()
		} else {
			return ErrKeyNotFound
		}
	})
}

func bucketIsEmpty(b *bolt.Bucket) bool {
	err := b.ForEach(func(k, v []byte) error {
		return ErrKeyFound
	})

	return err == nil
}

func copyKeyWithFile(env *commandEnvironment) error {
	if len(env.args) != 2 {
		return ErrUsage
	}

	srcIsBolt := isBoltURI(env.args[0])
	destIsBolt := isBoltURI(env.args[1])

	if srcIsBolt && destIsBolt {
		return resolveBoltURI(env, env.args[0], false, func(srcLoc *bolt.Location) error {
			return resolveBoltURI(env, env.args[1], true, func(destLoc *bolt.Location) error {
				v := srcLoc.GetHere()
				if v == nil {
					return ErrKeyNotFound
				}

				return destLoc.PutHere(v)
			})
		})
	} else if !destIsBolt {
		return resolveBoltURI(env, env.args[0], false, func(loc *bolt.Location) error {
			v := loc.GetHere()
			if v == nil {
				return ErrKeyNotFound
			}

			return ioutil.WriteFile(env.args[1], v, 0644)
		})
	} else if !srcIsBolt {
		return resolveBoltURI(env, env.args[1], true, func(loc *bolt.Location) error {
			v, err := ioutil.ReadFile(env.args[0])
			if err != nil {
				return err
			}

			return loc.PutHere(v)
		})
	} else {
		return errors.New("at least one of src and dest must be a <bolt://...> URI")
	}
}

func listKeys(env *commandEnvironment) error {
	if len(env.args) != 1 {
		return ErrUsage
	}

	return resolveBoltURI(env, env.args[0], false, func(loc *bolt.Location) error {
		something := loc.ResolveHere()
		var listKeysOf bolt.Bucketish

		if b, ok := something.(*bolt.Bucket); ok && b != nil {
			fmt.Printf("[is a bucket]\n")
			listKeysOf = b
		} else if rb, ok := something.(*bolt.RootBucket); ok && rb != nil {
			fmt.Printf("[is a root bucket]\n")
			listKeysOf = rb
		} else if v, ok := something.([]byte); ok && v != nil {
			fmt.Printf("[is a scalar value]\n")
			return nil
		} else {
			return ErrKeyNotFound
		}

		return listKeysOf.ForEach(func(k []byte, v []byte) error {
			if v == nil {
				fmt.Printf("%#x (bucket)\n", k)
			} else if len(v) < 50 {
				fmt.Printf("%#x = %#x\n", k, v)
			} else {
				fmt.Printf("%#x = <%d bytes>\n", k, len(v))
			}
			return nil
		})
	})
}

func printBucketTree(env *commandEnvironment) (err error) {
	maxDepth := int64(-1)
	if len(env.args) >= 2 && (env.args[0] == "-d" || env.args[0] == "--max-depth") {
		maxDepth, err = strconv.ParseInt(env.args[1], 10, 64)
		if err != nil {
			return err
		}
		env.args = env.args[2:]
	}

	if len(env.args) != 1 {
		return ErrUsage
	}

	return resolveBoltURI(env, env.args[0], false, func(loc *bolt.Location) error {
		something := loc.ResolveHere()
		var bish bolt.Bucketish

		if b, ok := something.(*bolt.Bucket); ok && b != nil {
			bish = b
		} else if rb, ok := something.(*bolt.RootBucket); ok && rb != nil {
			bish = rb
		} else {
			return ErrBucketNotFound
		}

		printBucketTreeNode(bish, 0, maxDepth)

		return nil
	})
}

func printBucketTreeNode(bish bolt.Bucketish, atDepth int64, maxDepth int64) {
	if atDepth == maxDepth {
		return
	}

	indentStr := strings.Repeat(" ", int(atDepth*2))

	bish.ForEach(func(k []byte, v []byte) error {
		if v == nil {
			fmt.Printf("%s%#x/\n", indentStr, k)
			printBucketTreeNode(bish.Bucket(k), atDepth+1, maxDepth)
		} else {
			fmt.Printf("%s%#x\n", indentStr, k)
		}
		return nil
	})
}

func diskUsage(env *commandEnvironment) error {
	if len(env.args) != 1 {
		return ErrUsage
	}

	return resolveBoltURI(env, env.args[0], false, func(loc *bolt.Location) error {
		something := loc.ResolveHere()
		var bish bolt.Bucketish

		if b, ok := something.(*bolt.Bucket); ok && b != nil {
			bish = b
		} else if rb, ok := something.(*bolt.RootBucket); ok && rb != nil {
			bish = rb
		} else {
			return ErrBucketNotFound
		}

		printDiskUsageOfNode(bish, 0, -1)

		return nil
	})
}

func printDiskUsageOfNode(bish bolt.Bucketish, atDepth int64, maxDepth int64) {
	if atDepth == maxDepth {
		return
	}

	indentStr := strings.Repeat(" ", int(atDepth*2))

	bish.ForEach(func(k []byte, v []byte) error {
		if v == nil {
			sb := bish.Bucket(k)
			fmt.Printf("%s%#x = %s\n", indentStr, k, formatByteSize(sb.StandaloneSize()))
			printDiskUsageOfNode(sb, atDepth+1, maxDepth)
		}
		return nil
	})
}

func formatByteSize(size uint64) string {
	switch getExp(size) {
	case 0:
		return "0"
	case 1:
		return fmt.Sprintf("%db", size)
	case 2:
		return fmt.Sprintf("%.1fK", float64(size)/1024)
	case 3:
		return fmt.Sprintf("%.1fM", float64(size)/(1024*1024))
	case 4:
		return fmt.Sprintf("%.1fG", float64(size)/(1024*1024*1024))
	default:
		return fmt.Sprintf("%.1fT", float64(size)/(1024*1024*1024*1024))
	}
}

func getExp(size uint64) (exp int) {
	for size > 0 {
		exp += 1
		size /= 1024
	}
	return
}
