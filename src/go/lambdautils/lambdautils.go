package lambdautils

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
)

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
