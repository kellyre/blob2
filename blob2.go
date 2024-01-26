package main

/***********************************************************************************************
Purpose:
  This program downloads blobs from Azure Storage and prepares them for processing by Splunk HEC or other tools.
  It is intended to be run from a Splunk add-on or other tool that can run a script on a schedule.
************************************************************************************************/

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type Config struct {
	StorageAccounts []StorageAccount `json:"storageAccounts"`
	Containers      []Container      `json:"containers"`
	Stages          []Stage          `json:"stages"`
	Maintenance     Maintenance      `json:"maintenance"`
}

type StorageAccount struct {
	Name    string `json:"name"`
	AuthKey string `json:"authKey"`
}

type Container struct {
	Name       string `json:"name"`
	Regex      string `json:"regex"`
	CaptureIdx int    `json:"captureIdx"`
}

type Stage struct {
	Name        string `json:"name"`
	Folder      string `json:"folder"`
	ThreadCount int    `json:"threadCount"`
}

type Maintenance struct {
	MinDiskFreePercent int    `json:"minDiskFreePercent"`
	DataFolder         string `json:"dataFolder"`
}

var (
	ctx        = context.Background()
	latestTime time.Time
)

func load_worker(wg *sync.WaitGroup, messageChannel <-chan string) {
	defer wg.Done()
	for i := range messageChannel {
		time.Sleep(time.Second)
		fmt.Println("done processing - ", i)
	}
}

func staging_worker(wg *sync.WaitGroup, messageChannel <-chan string) {
	defer wg.Done()
	for i := range messageChannel {
		time.Sleep(time.Second)
		fmt.Println("done processing - ", i)
	}
}

func completed_worker(wg *sync.WaitGroup, messageChannel <-chan string) {
	defer wg.Done()
	for i := range messageChannel {
		time.Sleep(time.Second)
		fmt.Println("done processing - ", i)
	}
}

func main() {
	config, err := loadConfig("blob_loader_config.json")
	if err != nil {
		panic(err)
	}

	/*
	 This section will create worker groups for all three stages and start them. The worker groups will
	 process blobs from the storage accounts and containers specified in the configuration file. The three
	 stages are: load, staging and completed. The load stage will download blobs from Azure Storage and
	 save them to the local disk. The staging stage will move blobs from the load folder to the staging
	 folder. The completed stage will move blobs from the staging folder to the completed folder.
	*/

	// Create worker groups for each stage
	load_wg := new(sync.WaitGroup)
	staging_wg := new(sync.WaitGroup)
	completed_wg := new(sync.WaitGroup)

	// Create channels for each stage
	load_channel := make(chan string)
	staging_channel := make(chan string)
	completed_channel := make(chan string)

	// loop starting workers with channels
	for _, stage := range config.Stages {
		switch stage.Name {
		case "load":
			for i := 0; i < stage.ThreadCount; i++ {
				load_wg.Add(1)
				go load_worker(load_wg, load_channel)
			}
		case "staging":
			for i := 0; i < stage.ThreadCount; i++ {
				staging_wg.Add(1)
				go staging_worker(staging_wg, staging_channel)
			}
		case "completed":
			for i := 0; i < stage.ThreadCount; i++ {
				completed_wg.Add(1)
				go completed_worker(completed_wg, completed_channel)
			}
		}
	}

	/*	for _, stage := range config.Stages {
			// Create a worker group for each stage
			wg := worker.NewGroup(stage.Name, stage.ThreadCount)

			// Start the worker group
			wg.Start()
			defer wg.Stop()
		}
	*/
	for _, account := range config.StorageAccounts {
		for _, container := range config.Containers {
			// Process blobs for each storage account and container
			processBlobs(account, container, config.Stages, config.Maintenance)
		}
	}
}

func loadConfig(filename string) (Config, error) {
	var config Config

	file, err := os.Open(filename)
	if err != nil {
		return config, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return config, err
	}

	err = json.Unmarshal(data, &config)
	if err != nil {
		return config, err
	}

	return config, nil
}

func processBlobs(account StorageAccount, container Container, stages []Stage, maintenance Maintenance) {
	// TODO: Implement blob processing logic for each storage account and container
	// Use the provided configuration parameters
}

/*
func extractTimeFromBlobName(blobName string, regex string, captureIdx int) (time.Time, error) {
	re := regexp.MustCompile(regex)

	matches := re.FindStringSubmatch(blobName)
	if matches == nil || len(matches) <= captureIdx {
		return time.Time{}, fmt.Errorf("unable to extract time from blob name")
	}

	timeStr := matches[captureIdx]
	blobTime, err := time.Parse("2006-01-02_15-04-05", timeStr)
	if err != nil {
		return time.Time{}, err
	}

	return blobTime, nil
}
*/

/*
var (
	accountName   = os.Getenv("STOR_ACCOUNT_NAME")
	accountKey    = os.Getenv("STOR_ACCOUNT_KEY")
	containerName = os.Getenv("STOR_CONTAINER_NAME")
	serviceURL    = "https://" + accountName + ".blob.core.windows.net"
)
*/

const (
	lagDuration  = time.Duration(5) * time.Minute // replace 5 with your desired lag in minutes
	oldThreshold = time.Duration(24) * time.Hour
)

func load_container_files(storage_account *StorageAccount, container *Container, processTime time.Time) {
	credential, err := azblob.NewSharedKeyCredential(storage_account.Name, storage_account.AuthKey)
	if err != nil {
		panic(err)
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", storage_account.Name))
	serviceURL := azblob.NewServiceURL(*URL, p)
	containerURL := serviceURL.NewContainerURL(container.Name)

	for {
		// Get blobs
		blobs, err := containerURL.ListBlobsFlatSegment(ctx, azblob.Marker{}, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			panic(err)
		}

		// Process blobs
		for _, blobInfo := range blobs.Segment.BlobItems {
			blobTime, err := extractTimeFromBlobName(blobInfo.Name)
			if err != nil {
				fmt.Println("Error extracting time:", err)
				continue
			}

			// Skip old blobs or blobs newer than the lag
			now := time.Now().UTC()
			if blobTime.Before(now.Add(-oldThreshold)) || blobTime.After(now.Add(-lagDuration)) {
				continue
			}

			fileName := blobTime.Format("2006-01-02_15-04-05.json")
			file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				fmt.Println("Error opening file:", err)
				continue
			}

			blobURL := containerURL.NewBlobURL(blobInfo.Name)
			resp, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
			if err != nil {
				fmt.Println("Error downloading blob:", err)
				file.Close()
				continue
			}

			// Save to file
			io.Copy(file, resp.Body(azblob.RetryReaderOptions{MaxRetryRequests: 3}))
			file.Close()
		}

		time.Sleep(1 * time.Minute)
	}
}

func extractTimeFromBlobName(blobName string) (time.Time, error) {
	pattern := `y=(\d+)/m=(\d+)/d=(\d+)/h=(\d+)/m=(\d+)/PT1H.json`
	re := regexp.MustCompile(pattern)

	matches := re.FindStringSubmatch(blobName)
	if matches == nil || len(matches) < 6 {
		return time.Time{}, fmt.Errorf("invalid blob name format, %q\n", matches)
	}

	year, _ := strconv.Atoi(matches[1])
	month, _ := strconv.Atoi(matches[2])
	day, _ := strconv.Atoi(matches[3])
	hour, _ := strconv.Atoi(matches[4])
	minute, _ := strconv.Atoi(matches[5])

	return time.Date(year, time.Month(month), day, hour, minute, 0, 0, time.UTC), nil
}
