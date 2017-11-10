package awsglacier

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/antontsv/backup/cloud"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glacier"
	ini "gopkg.in/ini.v1"
)

type service struct {
	vault *string
	*glacier.Glacier
}

const (
	// MB defines megabyte
	MB = 1024 * 1024
)

func (svc *service) upload(ctx context.Context, file *os.File, name string) (*glacier.ArchiveCreationOutput, error) {
	return svc.multiPartUpload(ctx, file, name)
}

func (svc *service) singleUpload(ctx context.Context, file *os.File, name string) (*glacier.ArchiveCreationOutput, error) {
	if name == "" {
		return nil, fmt.Errorf("Name is required for single file upload")
	}
	return svc.UploadArchiveWithContext(ctx, &glacier.UploadArchiveInput{
		AccountId:          aws.String("-"),
		ArchiveDescription: aws.String(name),
		VaultName:          svc.vault,
		Body:               file,
	})
}

func getTreeHash(r io.ReadSeeker) *string {
	h := glacier.ComputeHashes(r)
	s := fmt.Sprintf("%x", h.TreeHash)
	return &s
}

func (svc *service) multiPartUpload(ctx context.Context, file *os.File, name string) (*glacier.ArchiveCreationOutput, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot open get file info: %v", err)
	}
	if name == "" {
		name = info.Name()
	}
	size := info.Size()
	chunkSize := int64(16 * MB)
	chunks := int(size / chunkSize)
	if chunks < 1 {
		return svc.singleUpload(ctx, file, name)
	}

	out, err := svc.InitiateMultipartUploadWithContext(ctx, &glacier.InitiateMultipartUploadInput{
		AccountId:          aws.String("-"),
		ArchiveDescription: aws.String(name),
		VaultName:          svc.vault,
		PartSize:           aws.String(fmt.Sprintf("%d", chunkSize)),
	})
	if err != nil {
		return nil, err
	}
	id := out.UploadId

	errorc := make(chan error)
	sem := make(chan int, 10)
	wg := &sync.WaitGroup{}

	uploadPart := func(chunkNum int, file *os.File, uploadID *string, start, end, chunkSize int64) {
		r := io.NewSectionReader(file, start, chunkSize)
		defer wg.Done()
		_, err := svc.UploadMultipartPartWithContext(ctx, &glacier.UploadMultipartPartInput{
			AccountId: aws.String("-"),
			VaultName: svc.vault,
			UploadId:  uploadID,
			Range:     aws.String(fmt.Sprintf("bytes %d-%d/*", start, end)),
			Body:      r,
			Checksum:  getTreeHash(r),
		})
		<-sem
		if err != nil {
			errorc <- err
		}
	}

	for start, end, i := int64(0), chunkSize-1, 0; start < size; start, end, i = start+chunkSize, end+chunkSize, i+1 {
		if end > size {
			end = size - 1
		}
		n := end - start + 1
		sem <- 1
		wg.Add(1)
		go uploadPart(i, file, id, start, end, n)
	}
	go func() {
		wg.Wait()
		close(errorc)
	}()

	errs := ""
	for err := range errorc {
		if err != nil {
			errs = fmt.Sprintf("chunk error: %v; %s", err, errs)
		}
	}

	if ctx.Err() == context.Canceled {
		errs = "Operation was canceled"
	}

	if errs != "" {
		return nil, fmt.Errorf(errs)
	}

	return svc.CompleteMultipartUploadWithContext(ctx, &glacier.CompleteMultipartUploadInput{
		AccountId:   aws.String("-"),
		VaultName:   svc.vault,
		UploadId:    id,
		Checksum:    getTreeHash(file),
		ArchiveSize: aws.String(fmt.Sprintf("%d", size)),
	})
}

type amazonBackup struct {
	ctx      context.Context
	svc      *service
	bucket   string
	settings map[string]string
}

// New returns a Backuper that works with Amazon glacier
func New(ctx context.Context, bucketName string, cnf *ini.Section) (cloud.Backuper, error) {
	values, err := cloud.GetKeyValues([]string{"region"}, cnf)
	if err != nil {
		return nil, err
	}
	svc := &service{vault: aws.String(bucketName)}
	svc.Glacier = glacier.New(session.New(&aws.Config{
		Region: aws.String(values["region"]),
	}))

	_, err = svc.CreateVault(&glacier.CreateVaultInput{
		VaultName: svc.vault,
	})
	if err != nil {
		return nil, fmt.Errorf("cannot create Amazon vault: %v", err)
	}

	return &amazonBackup{
		ctx:      ctx,
		svc:      svc,
		bucket:   bucketName,
		settings: values,
	}, err
}

func (b *amazonBackup) Upload(ctx context.Context, file string, dest string) error {
	source, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("cannot open file %s in order to start backup into Amazon Glacier: %v", file, err)
	}

	location := ""

	if !strings.HasSuffix(dest, "/") && dest != "." && len(dest) > 0 {
		location = dest
	}

	_, err = b.svc.upload(ctx, source, location)

	return err
}
