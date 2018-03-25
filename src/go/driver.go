package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seongju/lambda-refarch-mapreduce/src/go/lambdautils"
)

type LambdaFunction struct {
	Name    string `json:"name"`
	Handler string `json:"handler"`
	Zip     string `json:"zip"`
}

type ConfigFile struct {
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

type JobInfo struct {
	JobID             string
	JobBucket         string
	ReducerLambdaName string
	ReducerHandler    string
	numMappers        int
}

func writeJobConfig(jobID, jobBucket, reducerLambdaName, reducerHandler string, numMappers int) error {
	fileName := "jobconfig.json"
	jobInfo := JobInfo{
		jobID,
		jobBucket,
		reducerLambdaName,
		reducerHandler,
		numMappers,
	}
	jobInfoJSON, err := json.Marshal(jobInfo)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(fileName, jobInfoJSON, 0644)
	return err
}

// This function is from
// https://golangcode.com/create-zip-files-in-go/
func zipLambda(lambdaFileName, zipName string, c chan error) {
	newFile, err := os.Create(zipName)
	if err != nil {
		c <- err
		return
	}

	zipWriter := zip.NewWriter(newFile)

	lambdaFile, err := os.Open(lambdaFileName)
	if err != nil {
		zipWriter.Close()
		newFile.Close()
		c <- err
		return
	}

	info, err := lambdaFile.Stat()
	if err != nil {
		lambdaFile.Close()
		zipWriter.Close()
		newFile.Close()
		c <- err
		return
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		lambdaFile.Close()
		zipWriter.Close()
		newFile.Close()
		c <- err
		return
	}

	// Change to deflate to gain better compression
	// see http://golang.org/pkg/archive/zip/#pkg-constants
	header.Method = zip.Deflate

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		lambdaFile.Close()
		zipWriter.Close()
		newFile.Close()
		c <- err
		return
	}

	_, err = io.Copy(writer, lambdaFile)
	lambdaFile.Close()
	zipWriter.Close()
	newFile.Close()
	c <- err
}

func main() {
	//  JOB ID
	jobID := os.Args[1]
	fmt.Printf("Starting job %s\n", jobID)

	// Retrieve the values in driverconfig.json
	raw, err := ioutil.ReadFile("./driverconfig.json")
	if err != nil {
		panic(err)
	}
	var config ConfigFile
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
	batches := lambdautils.BatchCreator(allObjects, objectsPerBatch)
	numMappers := len(batches)

	lambdaPrefix := "BL"
	mapperLambdaName := lambdaPrefix + "-mapper-" + jobID
	reducerLambdaName := lambdaPrefix + "-reducer-" + jobID
	reducerCoordinatorLambdaName := lambdaPrefix + "-reducerCoordinator-" + jobID

	err = writeJobConfig(jobID, jobBucket, reducerLambdaName, config.Reducer.Handler, numMappers)
	if err != nil {
		panic(err)
	}

	c := make(chan error, 3)
	go zipLambda(config.Mapper.Name, config.Mapper.Zip, c)
	go zipLambda(config.Reducer.Name, config.Reducer.Zip, c)
	go zipLambda(config.ReducerCoordinator.Name, config.ReducerCoordinator.Zip, c)

	for i := 0; i < 3; i++ {
		err = <-c
		if err != nil {
			panic(err)
		}
	}

	lambdaClient := lambda.New(sess)
	mapperLambdaManager := &lambdautils.LambdaManager{
		LambdaClient: lambdaClient,
		S3Client:     s3Client,
		Region:       "us-east-1",
		PathToZip:    config.Mapper.Zip,
		JobID:        jobID,
		LambdaName:   mapperLambdaName,
		HandlerName:  config.Mapper.Handler,
		Role:         os.Getenv("serverless_mapreduce_role"),
		LambdaMemory: 1536,
		Timeout:      300,
	}
	err = mapperLambdaManager.CreateOrUpdateLambda()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Mapper Function ARN: %s\n", mapperLambdaManager.FunctionArn)

	reducerLambdaManager := &lambdautils.LambdaManager{
		LambdaClient: lambdaClient,
		S3Client:     s3Client,
		Region:       "us-east-1",
		PathToZip:    config.Reducer.Zip,
		JobID:        jobID,
		LambdaName:   reducerLambdaName,
		HandlerName:  config.Reducer.Handler,
		Role:         os.Getenv("serverless_mapreduce_role"),
		LambdaMemory: 1536,
		Timeout:      300,
	}
	err = reducerLambdaManager.CreateOrUpdateLambda()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Reducer Function ARN: %s\n", reducerLambdaManager.FunctionArn)

	reducerCoordLambdaManager := &lambdautils.LambdaManager{
		LambdaClient: lambdaClient,
		S3Client:     s3Client,
		Region:       "us-east-1",
		PathToZip:    config.ReducerCoordinator.Zip,
		JobID:        jobID,
		LambdaName:   reducerCoordinatorLambdaName,
		HandlerName:  config.ReducerCoordinator.Handler,
		Role:         os.Getenv("serverless_mapreduce_role"),
		LambdaMemory: 1536,
		Timeout:      300,
	}

	err = reducerCoordLambdaManager.CreateOrUpdateLambda()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Reducer Coordinator Function ARN: %s\n", reducerCoordLambdaManager.FunctionArn)
	
	// Give the bucket invoke permission on the lambda
	err = reducerCoordLambdaManager.AddLambdaPermission(string(rand.Intn(1000)), "arn:aws:s3:::" + bucket)
	if err != nil {
		panic(err)
	}

}
