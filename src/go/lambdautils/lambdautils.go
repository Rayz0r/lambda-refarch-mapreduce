package lambdautils

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
)

func ComputeBatchSize(contents []*s3.Object, lambdaMemory int) int {
	totalSizeOfDataset := 0.0

	for _, content := range contents {
		totalSizeOfDataset += float64(*content.Size)
	}

	avgObjectSize := totalSizeOfDataset/float64(len(contents))
	fmt.Printf("Dataset size (bytes) %f, nKeys: %d, avg size (bytes): %f", totalSizeOfDataset, len(contents), avgObjectSize)
	maxMemoryForDataset := 0.6*float64(lambdaMemory*1000*1000)
	filesPerBatch := int(maxMemoryForDataset/avgObjectSize) // Golang does not provide a round function in standard math
	return filesPerBatch
}