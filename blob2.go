package main

/***********************************************************************************************
Purpose:
  This program downloads blobs from Azure Storage and prepares them for processing by Splunk HEC or other tools.
  It is intended to be run from a Splunk add-on or other tool that can run a script on a schedule.
************************************************************************************************/

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

var (
	accountName   = os.Getenv("STOR_ACCOUNT_NAME")
	accountKey    = os.Getenv("STOR_ACCOUNT_KEY")
	containerName = os.Getenv("STOR_CONTAINER_NAME")
	serviceURL    = "https://" + accountName + ".blob.core.windows.net"
)

const (
	lagDuration  = time.Duration(5) * time.Minute // replace 5 with your desired lag in minutes
	oldThreshold = time.Duration(24) * time.Hour
)

var (
	ctx        = context.Background()
	latestTime time.Time
)

func main() {
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	URL, _ := url.Parse(serviceURL)
	serviceURL := azblob.NewServiceURL(*URL, p)
	containerURL := serviceURL.NewContainerURL(containerName)

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
