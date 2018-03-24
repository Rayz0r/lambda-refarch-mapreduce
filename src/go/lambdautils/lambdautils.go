package lambdautils

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
)

type LambdaManager struct {
	LambdaClient                                                         *lambda.Lambda
	S3Client                                                             *s3.S3
	Region, PathToZip, JobID, LambdaName, HandlerName, Role, FunctionArn string
	LambdaMemory, Timeout                                                int64
}

func (lm *LambdaManager) CreateLambdaFunction() error {
	runtime := "python2.7"
	createFunctionInput := &lambda.CreateFunctionInput{
		FunctionName: &lm.LambdaName,
		Handler:      &lm.HandlerName,
		Role:         &lm.Role,
		Runtime:      &runtime,
		MemorySize:   &lm.LambdaMemory,
		Timeout:      &lm.Timeout,
	}

	zipFileBytes, err := ioutil.ReadFile(lm.PathToZip)
	if err != nil {
		return err
	}

	functionCode := &lambda.FunctionCode{
		ZipFile: zipFileBytes,
	}
	createFunctionInput = createFunctionInput.SetCode(functionCode)

	functionConfig, err := lm.LambdaClient.CreateFunction(createFunctionInput)
	if err != nil {
		return err
	}
	lm.FunctionArn = *functionConfig.FunctionArn
	return err
}

func ComputeBatchSize(allObjects []*s3.Object, lambdaMemory int) int {
	totalSizeOfDataset := 0.0

	for _, object := range allObjects {
		totalSizeOfDataset += float64(*object.Size)
	}

	avgObjectSize := totalSizeOfDataset / float64(len(allObjects))
	fmt.Printf("Dataset size (bytes) %f, nKeys: %d, avg size (bytes): %f\n", totalSizeOfDataset, len(allObjects), avgObjectSize)
	maxMemoryForDataset := 0.6 * float64(lambdaMemory*1000*1000)
	objectsPerBatch := int(maxMemoryForDataset / avgObjectSize) // Golang does not provide a round function in standard math
	return objectsPerBatch
}

func BatchCreator(allObjects []*s3.Object, objectsPerBatch int) [][]string {
	var batches [][]string
	var batch []string
	for _, object := range allObjects {
		batch = append(batch, *object.Key)
		if len(batch) == objectsPerBatch {
			batches = append(batches, batch)
			batch = []string{}
		}
	}

	// If there are objects in batch
	if len(batch) > 0 {
		batches = append(batches, batch)
	}
	return batches
}
