package splits

import (
	"bufio"
	csx "encoding/csv"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"trimmer.io/go-csv"
)

type ICsvProcessor interface {
	ProcessCsv(filePath string, config *Config) (string, error)
}

type CsvProcess struct{}

func New() ICsvProcessor {
	return &CsvProcess{}
}

type By func(p1, p2 *Record) bool

func Find(slice []Record, val string) (int, bool) {
	for i, item := range slice {
		if item.Nimi == val {
			return i, true
		}
	}
	return -1, false
}

//-------------------------------------------------------
//processing big file to split into small files
//-------------------------------------------------------
func (p *CsvProcess) ProcessCsv(filePath string, config *Config) (string, error) {

	wg := sync.WaitGroup{}
	file, err := os.Open(filePath)
	defer file.Close()

	if err != nil {
		return "", err
	}
	reader := csv.NewDecoder(bufio.NewReader(file)).SkipUnknown(true)
	destinationPath := fmt.Sprintf("%s%s%s", config.Destination, config.DirectorySep, time.Now().Format("02-Jan-2006"))

	if _, err = os.Stat(destinationPath); os.IsNotExist(err) {
		err = os.Mkdir(destinationPath, os.ModeDir)
	}
	Error("error in day directory creation", err)
	_, filename := filepath.Split(filePath)

	fileFolderPath := fmt.Sprintf("%s%s%s", destinationPath, config.DirectorySep, removeSpecialChars(filename))
	if _, err = os.Stat(fileFolderPath); os.IsNotExist(err) {
		err = os.Mkdir(fileFolderPath, os.ModeDir)
	}
	Error("error in file directory creation", err)

	var itemTable [][]string
	var initialItem []Record
	initialItem = []Record{}
	for {
		recordOrd, err := reader.ReadLine()
		if err == io.EOF || recordOrd == "" {
			break
		}
		Error("error in day directory creation", err)
		record := strings.Split(recordOrd, ",")
		if record[1] != "" && record[1] != " " {
			itemTable = append(itemTable, record)
		}
	}
	record := Record{Data: &[][]string{}}
	counter := 0

	for _, item := range itemTable {
		if item[0] == "200" {
			index, exists := Find(initialItem, item[1])
			if exists {
				record = initialItem[index]
			} else {
				record = Record{Data: &[][]string{}}
			}
			record.Nimi = item[1]
			if !exists {

				nmiFileName := fmt.Sprintf("%s%s", "", item[1])
				firstElement := []string{"100", nmiFileName, config.Client, config.Client, "\r\n"}
				*record.Data = append(*record.Data, firstElement)
				initialItem = append(initialItem, record)
			}
		}
		*record.Data = append(*record.Data, item)
		counter++
		if counter < len(itemTable) && itemTable[counter][0] == "200" {
			*record.Data = append(*record.Data, []string{"900", "\r\n"})
		}
	}
	var m sync.RWMutex
	listChan := make(chan [][]string)
	for _, data := range initialItem {
		if len(*data.Data) > 0 {
			item := *data.Data
			nmiFileName := fmt.Sprintf("%s%s", "", data.Nimi)
			wg.Add(1)
			go ProcessMeterDataSplitting(listChan, &m, &wg, nmiFileName, fileFolderPath, config)
			listChan <- item
			wg.Wait()
		}
	}
	ss := fmt.Sprintf("%s%s%s", fileFolderPath, config.DirectorySep, filename)
	return ss, nil
}

//--------------------------------------------------------
//go routing to create file
//---------------------------------------------------------
func ProcessMeterDataSplitting(arr <-chan [][]string, m *sync.RWMutex, wg *sync.WaitGroup, nimiNumber string, destinationPath string, config *Config) {
	defer wg.Done()
	m.Lock()
	select {
	case val := <-arr:
		uid, eru := uuid.NewV4()
		Error("unique id error", eru)

		file, err := os.Create(fmt.Sprintf("%s%s%s%s%s", destinationPath, config.DirectorySep, nimiNumber+"-", uid.String(), ".csv"))
		Error("error in file creation", err)
		defer file.Close()
		writer := csx.NewWriter(file)
		defer writer.Flush()
		for _, item := range val {
			fmt.Println(item)
			err = writer.Write(item)
			Error("Error in writing to file", err)
		}
	}
	m.Unlock()
}

func Error(message string, err error) {

	if err := recover(); err != nil {
		fmt.Println(err)
	}
}

//-----------------------------------------------
// Remove special characters from path
//-----------------------------------------------
func removeSpecialChars(data string) string {
	reg, err := regexp.Compile("[^a-zA-Z0-9]+")
	Error("error in regular expression", err)
	processedString := reg.ReplaceAllString(data, "")
	return processedString
}

func MoveFile(filePath string, newPath string) {
	err := os.Rename(filePath, newPath)
	Error("error in moving file", err)

}

func Panicking() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()
}
