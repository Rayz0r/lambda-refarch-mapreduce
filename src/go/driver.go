package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/seongju/lambda-refarch-mapreduce/src/go/lambdautils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type LambdaFunction struct {
	Name    string `json:"name"`
	Handler string `json:"handler"`
	Zip     string `json:"zip"`
}

type configFile struct {
	Bucket             string         `json:"bucket"`
	Prefix             string         `json:"prefix"`
	JobBucket          string         `json:"jobBucket"`
	Region             string         `json:"region"`
	LambdaMemory       int            `json:"lambdaMemory"`
	ConcurrentLambdas  int            `json:"concurrentLambdas"`
	Mapper             LambdaFunction `json:"mapper"`
	Reducer            LambdaFunction `json:"reducer"`
	ReducerCoordinator LambdaFunction `json:"reducerCoordinator"`
}

func main() {
	//  JOB ID
	jobID := os.Args[1]
	fmt.Printf("Starting job %s", jobID)

	// Retrieve the values in driverconfig.json
	raw, err := ioutil.ReadFile("./driverconfig.json")
	if err != nil {
		panic(err)
	}
	var config configFile
	err = json.Unmarshal(raw, &config)
	if err != nil {
		panic(err)
	}

	bucket := config.Bucket
	jobBucket := config.JobBucket
	region := config.Region
	lambdaMemory := config.LambdaMemory
	concurrentLambdas := config.ConcurrentLambdas

	fmt.Println(bucket)
	fmt.Println(jobBucket)
	fmt.Println(region)
	fmt.Println(lambdaMemory)
	fmt.Println(concurrentLambdas)

	// Fetch the keys that match the prefix from the config
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	s3Client := s3.New(sess)
	listObjectsInput := new(s3.ListObjectsInput)
	listObjectsInput = listObjectsInput.SetBucket(bucket)
	listObjectsInput = listObjectsInput.SetPrefix(config.Prefix)
	var maxKeys int64 = 1000
	listObjectsInput = listObjectsInput.SetMaxKeys(maxKeys)
	listObjectsOutput, err := s3Client.ListObjects(listObjectsInput)
	if err != nil {
		panic(err)
	}
	allObjects := listObjectsOutput.Contents

	objectsPerBatch := lambdautils.ComputeBatchSize(allObjects, lambdaMemory)
	fmt.Println(objectsPerBatch)
	batches := lambdautils.BatchCreator(allObjects, objectsPerBatch)
	fmt.Println(batches)
}
