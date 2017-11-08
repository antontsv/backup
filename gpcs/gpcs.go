package gpcs

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/antontsv/backup/cloud"
	"google.golang.org/api/iterator"
	ini "gopkg.in/ini.v1"

	"cloud.google.com/go/storage"
)

const storageClass = "COLDLINE"

type googleBackup struct {
	ctx      context.Context
	handle   *storage.BucketHandle
	bucket   string
	settings map[string]string
}

// New returns a Backuper that works with Google Cloud Storage
func New(ctx context.Context, bucketName string, cnf *ini.Section) (cloud.Backuper, error) {
	values, err := cloud.GetKeyValues([]string{"projectID"}, cnf)
	if err != nil {
		return nil, err
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot create Google storage client: %v", err)
	}

	var verifiedBucket string

	i := client.Buckets(ctx, values["projectID"])
	i.Prefix = bucketName
	for bucketAttr, err := i.Next(); err != iterator.Done; bucketAttr, err = i.Next() {
		if err != nil {
			return nil, fmt.Errorf("bucket search error: %v", err)
		}
		if bucketAttr.Name == bucketName {
			if bucketAttr.StorageClass != storageClass {
				return nil, fmt.Errorf("refusing to use provided bucket due to storage class missmatch. Expected storage type %s, but got %s", storageClass, bucketAttr.StorageClass)
			}
			verifiedBucket = bucketAttr.Name
			break
		}
	}

	handle := client.Bucket(bucketName)

	if verifiedBucket == "" {
		attrs := &storage.BucketAttrs{
			StorageClass: storageClass,
		}

		err = handle.Create(ctx, values["projectID"], attrs)
		if err != nil {
			return nil, fmt.Errorf("cannot create Google storage bucket: %v", err)
		}
		verifiedBucket = bucketName
	}

	return &googleBackup{
		ctx:      ctx,
		handle:   handle,
		bucket:   verifiedBucket,
		settings: values,
	}, err
}

func (b *googleBackup) Upload(ctx context.Context, file string, dest string) error {
	source, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("cannot open file %s in order to start backup into GPC: %v", file, err)
	}
	info, err := source.Stat()
	if err != nil {
		return fmt.Errorf("cannot open get %s file info: %v", file, err)
	}

	location := info.Name()

	if strings.HasSuffix(dest, "/") {
		location = fmt.Sprintf("%s%s", dest, location)
	} else if dest != "." && len(dest) > 0 {
		location = dest
	}

	f := b.handle.Object(location)
	w := f.NewWriter(ctx)

	buf := make([]byte, 1024)
	for {
		n, err := source.Read(buf)
		if n > 0 {
			_, err = w.Write(buf[0:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error during %s backup: %v", file, err)
		}
	}

	err = w.Close()
	if err != nil {
		return fmt.Errorf("error while finishing %s backup: %v", file, err)
	}

	return nil
}
