package mapper

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-lambda-go/lambda"
)

const TaskMapperPrefix = "task/mapper/"

func writeToS3(sess *session.Session, bucket, key string, data []byte) error {
	reader := bytes.NewReader(data)
	uploader := s3manager.NewUploader(sess)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: &bucket,
		Key:    &key,
	})
	return err
}

type Mapper struct {
	Name string `json:"name"`
}

func HandleRequest(ctx context.Context, name Mapper) (string, error) {
	return fmt.Sprintf("Hello %s!", name.Name), nil
}

func main() {
	lambda.Start(HandleRequest)
}
