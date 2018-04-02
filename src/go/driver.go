package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/seongju/lambda-refarch-mapreduce/src/go/lambdautils"
)

type JobData struct {
	MapCount     int     `json:"n_mapper"`
	TotalS3Files int     `json:"totalS3Files"`
	StartTime    float64 `json:"startTime"`
}

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

type LambdaPayload struct {
	Bucket    string   `json:"bucket"`
	Keys      []string `json:"keys"`
	JobBucket string   `json:"jobBucket"`
	JobID     string   `json:"jobId"`
	MapperID  int      `json:"mapperId"`
}

type InvokeLambdaResult struct {
	Payload []string
	Error   error
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

// This function is heavily influenced by
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

func invokeLambda(lambdaClient *lambda.Lambda, batch []string, mapperID int, mapperLambdaName *string, bucket, jobBucket, jobID string, c chan InvokeLambdaResult) {
	var result []string
	payload, err := json.Marshal(LambdaPayload{
		Bucket:    bucket,
		Keys:      batch,
		JobBucket: jobBucket,
		JobID:     jobID,
		MapperID:  mapperID,
	})

	if err != nil {
		c <- InvokeLambdaResult{result, err}
		return
	}

	invokeInput := &lambda.InvokeInput{
		FunctionName: mapperLambdaName,
		Payload:      payload,
	}

	invokeOutput, err := lambdaClient.Invoke(invokeInput)
	if err != nil {
		c <- InvokeLambdaResult{result, err}
		return
	}

	err = json.Unmarshal(invokeOutput.Payload, &result)
	if err != nil {
		c <- InvokeLambdaResult{result, err}
		return
	}
	fmt.Println(result)
	c <- InvokeLambdaResult{result, nil}
	return
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
	err = reducerCoordLambdaManager.AddLambdaPermission(string(rand.Intn(1000)), "arn:aws:s3:::"+jobBucket)
	if err != nil {
		if awsErr, ok := err.(awserr.RequestFailure); ok && awsErr.StatusCode() == 409 {
			fmt.Println("Statement already exists on Reducer Coordinator")
		} else {
			panic(err)
		}
	}

	// Write job data to S3
	jobData := JobData{
		MapCount:     numMappers,
		TotalS3Files: len(allObjects),
		StartTime:    float64(time.Now().UnixNano()) * math.Pow10(-9),
	}
	jobDataJSON, err := json.Marshal(jobData)
	if err != nil {
		panic(err)
	}

	err = writeToS3(sess, jobBucket, jobID+"/jobdata", jobDataJSON)
	if err != nil {
		panic(err)
	}

	resultChannel := make(chan InvokeLambdaResult, numMappers)
	for mapperID := 0; mapperID < numMappers; mapperID += 1 {
		go invokeLambda(lambdaClient, batches[mapperID], mapperID, &mapperLambdaName, bucket, jobBucket, jobID, resultChannel)
	}

	var totalS3GetOps int
	var totalLines int
	var totalLambdaSecs float64

	for i := 0; i < numMappers; i += 1 {
		result := <-resultChannel
		if result.Error != nil {
			panic(result.Error)
		}
		// TODO should check for length of 4 in result.Payload to see if the Lambda returned an error
		s3GetOps, err := strconv.Atoi(result.Payload[0])
		if err != nil {
			panic(err)
		}
		totalS3GetOps += s3GetOps
		numLines, err := strconv.Atoi(result.Payload[1])
		if err != nil {
			panic(err)
		}
		totalLines += numLines
		seconds, err := strconv.ParseFloat(result.Payload[2], 64)
		if err != nil {
			panic(err)
		}
		totalLambdaSecs += seconds
	}
	fmt.Printf("Total seconds %f\nTotal lines %d\nTotal S3 operations %d\n", totalLambdaSecs, totalLines, totalS3GetOps)
}
