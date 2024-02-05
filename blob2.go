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
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type StorageAccount struct {
	Name    string `json:"name"`
	AuthKey string `json:"authKey"`
}

type Container struct {
	Name         string `json:"name"`
	Regex        string `json:"regex"`
	CaptureIdx   int    `json:"captureIdx"`
	LagDuration  int    `json:"lagDuration"`
	OldThreshold int    `json:"oldThreshold"`
}

type Stage struct {
	Name        string `json:"name"`
	Folder      string `json:"folder"`
	ThreadCount int    `json:"threadCount"`
}

type Config struct {
	StorageAccounts []StorageAccount `json:"storageAccounts"`
	Containers      []Container      `json:"containers"`
	Stages          []Stage          `json:"stages"`
	Maintenance     Maintenance      `json:"maintenance"`
}

type Task struct {
	Type          string
	DateString    string
	Stage         *Stage
	StorageAct    *StorageAccount
	ContainerInfo *Container
}

type TaskQueue struct {
}

type Maintenance struct {
	MinDiskFreePercent  int    `json:"minDiskFreePercent"`
	DataFolder          string `json:"dataFolder"`
	DefaultLagDuration  int    `json:"defaultLagDuration"`
	DefaultOldThreshold int    `json:"defaultOldThreshold"`
	TimerThreadWait     int    `json:"timerThreadWait"`
}

// StagePause is safe to use concurrently
type StagePause struct {
	mu sync.Mutex
	v  map[string]bool
}

// Pause a given stage by name
func (c *StagePause) Pause(key string) {
	c.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	c.v[key] = true
	c.mu.Unlock()
}

// Unpause a given stage by name
func (c *StagePause) Unpause(key string) {
	c.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	c.v[key] = false
	c.mu.Unlock()
}

// Value returns the current value of StagePause for the given key.
func (c *StagePause) CheckPaused(key string) bool {
	c.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer c.mu.Unlock()
	return c.v[key]
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

func extractResourceAndTimeFromBlobName(blobName string) (string, time.Time, error) {
	pattern := `^resourceId=(.*)/y=(\d+)/m=(\d+)/d=(\d+)/h=(\d+)/m=(\d+)/PT1H.json`
	re := regexp.MustCompile(pattern)
	resource := ""

	matches := re.FindStringSubmatch(blobName)
	if matches == nil || len(matches) < 6 {
		return resource, time.Time{}, fmt.Errorf("invalid blob name format, %q\n", matches)
	}

	resource = matches[1]
	year, _ := strconv.Atoi(matches[2])
	month, _ := strconv.Atoi(matches[3])
	day, _ := strconv.Atoi(matches[4])
	hour, _ := strconv.Atoi(matches[5])
	minute, _ := strconv.Atoi(matches[6])

	return resource, time.Date(year, time.Month(month), day, hour, minute, 0, 0, time.UTC), nil
}

func load_container_files(stage *Stage, storage_account *StorageAccount, container *Container, processTime time.Time) {

	credential, err := azblob.NewSharedKeyCredential(storage_account.Name, storage_account.AuthKey)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	/*	lagDuration := time.Duration(container.LagDuration) * time.Minute
		oldThreshold := time.Duration(container.OldThreshold) * time.Hour
	*/

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
			resource, blobTime, err := extractResourceAndTimeFromBlobName(blobInfo.Name)
			if err != nil {
				fmt.Println("Error extracting time:", err)
				continue
			}
			// Skip old blobs or blobs newer than the lag
			/*			now := time.Now().UTC()
						if blobTime.Before(now.Add(-oldThreshold)) || blobTime.After(now.Add(-lagDuration)) {
							continue
						}
			*/
			if blobTime != processTime {
				continue
			}

			fileName := blobTime.Format("2006-01-02_15-04-05.json")
			fullPath := filepath.Join(stage.Folder, resource, fileName)
			file, err := os.OpenFile(fullPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

func loaderTask(task *Task) Task {
	time.Sleep(10 * time.Second)
	return *task
}

func stagingTask(task *Task) Task {
	time.Sleep(10 * time.Second)
	return *task
}

func completedTask(task *Task) Task {
	time.Sleep(10 * time.Second)
	return *task
}

func worker(wg *sync.WaitGroup, tasks <-chan Task, results chan<- Task) {
	defer wg.Done()
	for task := range tasks {
		switch task.Stage.Name {
		case "timer":
			fmt.Printf("In timer\n")
			time.Sleep(2 * time.Second)
			results <- task
		case "loader":
			fmt.Printf("In loader\n")
			task = loaderTask(&task)
			results <- task
		case "staging":
			fmt.Printf("In staging\n")
			task = stagingTask(&task)
			results <- task
		case "completed":
			fmt.Printf("In completed\n")
			task = completedTask(&task)
			results <- task
		}
	}
}

/*
func interrogateContainer(account StorageAccount, container Container) *TaskQueue {
	// TODO: Go through container and populate container queue with list of
	// times to process
}
*/

func processBlobs(account StorageAccount, container Container, stages []Stage, maintenance Maintenance) {
	// TODO: Implement blob processing logic for each storage account and container
	// Use the provided configuration parameters
}

func main() {
	config, err := loadConfig("blob_loader_config.json")
	if err != nil {
		panic(err)
	}

	wg := new(sync.WaitGroup)

	worker_tracker := make(map[string]int)
	num_workers := 0
	for _, stage := range config.Stages {
		num_workers += stage.ThreadCount
		worker_tracker[stage.Name] = stage.ThreadCount
	}

	// Create channels for each workers
	tasks := make(chan Task, num_workers)
	results := make(chan Task, num_workers)

	// create workers
	for i := 0; i < num_workers; i++ {
		wg.Add(1)
		go worker(wg, tasks, results)

	}

	for {
		for _, account := range config.StorageAccounts {
			for _, container := range config.Containers {
				// Process blobs for each storage account and container
				processBlobs(account, container, config.Stages, config.Maintenance)
			}
		}
	}
}
