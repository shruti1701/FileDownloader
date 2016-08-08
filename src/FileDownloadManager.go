package main

import (
	"fmt"

	"time"

	"sync"

	"strconv"

	"net/http"
	"strings"

	"io/ioutil"

	"math/rand"
	"os"
)

const (
	fileName = "ubuntu-16.04-desktop-amd64.iso"
)

var availableClienNodes []string
var clientNodes = []string{"http://192.168.0.121/", "http://localhost/"}

func main() {
	var fileSize uint64
	//get available clients
	availableClienNodes, fileSize = getAvailableClients()
	n := len(availableClienNodes)

	fmt.Println("No of available servers", n)
	fmt.Println("Available Clients", availableClienNodes)

	var wg sync.WaitGroup

	var blockSize, startByte, lastByte uint64
	blockSize = fileSize / uint64(n)
	startByte = 0
	lastByte = blockSize
	rangeArray := make([][2]uint64, n)
	//divides the file into multiple chunks
	for j := 0; j < int(n); j++ {
		rangeArray[j][0] = startByte
		rangeArray[j][1] = lastByte
		startByte = lastByte + 1
		if j == int(n)-2 {
			lastByte = fileSize
		} else {
			lastByte = lastByte + blockSize
		}

	}
	fmt.Println(rangeArray)
	os.MkdirAll("temp", 0777)
	wg.Add(int(n))

	for i := 0; i < int(n); i++ {
		go downloadFileOverHttpAndWriteToLocalFile(&wg, rangeArray[i][0], rangeArray[i][1], availableClienNodes[i], "temp/Block"+strconv.Itoa(i)+".bin")
	}
	wg.Wait()
	var data, totalData []byte
	for i := 0; i < int(n); i++ {
		filepath := "temp/Block" + strconv.Itoa(i) + ".bin"
		data, _ = ioutil.ReadFile(filepath)
		totalData = append(totalData, data...)

	}
	ioutil.WriteFile(fileName, totalData, 0777)
	//remove the temporary files
	os.RemoveAll("temp")

}

//get the list of servers which can be connected and have the file to be downloaded available on the server
func getAvailableClients() ([]string, uint64) {
	availableClients := make([]string, 0)
	var filesize uint64

	for _, client := range clientNodes {
		size := make(chan uint64)
		go getFileSize(size, client, fileName)
		if sz := <-size; sz != 0 {
			availableClients = append(availableClients, client)
			filesize = sz

		}
	}
	return availableClients, filesize

}

// get the size of the file to download
func getFileSize(size chan<- uint64, clientID string, fileName string) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", clientID+fileName, nil)
	req.Header.Add("Range", "bytes=0-0")
	resp, err := client.Do(req)
	if err != nil {
		// handle error
		fmt.Println("Error occured while connecting to the server", clientID)
		size <- 0
		return
	}
	defer resp.Body.Close()
	fmt.Println(resp.Header.Get("Content-Range"))
	contentRange := resp.Header.Get("Content-Range")
	contentRange = contentRange[strings.Index(contentRange, "/")+1:]
	fmt.Println(contentRange)
	sz, _ := strconv.Atoi(contentRange)
	size <- uint64(sz)

}

//download the file from server
func downloadFileOverHttpAndWriteToLocalFile(wg *sync.WaitGroup, startByte uint64, lastByte uint64, clientAddress string, tempFileName string) {

	var response []byte
	blockSize := lastByte - startByte
	fmt.Println("Download of ", tempFileName, " of size ", blockSize, " Started From", clientAddress)
	for {

		byteRange := strconv.FormatUint(startByte, 10) + "-" + strconv.FormatUint(lastByte, 10)
		fmt.Println("Download ", "Block Range", byteRange, "from node ", clientAddress)
		client := &http.Client{
			Timeout: time.Second * 20,
		}
		req, err := http.NewRequest("GET", clientAddress+fileName, nil)
		req.Header.Add("Range", "bytes="+byteRange)
		resp, err := client.Do(req)
		if err != nil {
			// handle error
			fmt.Printf("%s", err)
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		response = append(response, body...)
		fmt.Println("Downloaded data of ", len(body), " bytes from ", startByte, "-", lastByte, " from ", clientAddress, len(response))

		if uint64(len(response)) >= blockSize && startByte <= lastByte {
			fmt.Println("File Download- ", tempFileName, " Completed From", clientAddress)
			break
		} else {
			startByte = uint64(len(body)) + startByte
			//randomly pics new client node from the availble list
			clientAddress = availableClienNodes[rand.Intn(len(availableClienNodes)-1)]
		}

	}
	//write the in memory data to file
	ioutil.WriteFile(tempFileName, response, 0777)
	wg.Done()
}
