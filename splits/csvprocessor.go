package splits

import (
	"bufio"
	csx "encoding/csv"
	"fmt"
	. "github.com/ahmetb/go-linq"
	"github.com/nu7hatch/gouuid"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
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

type By func(p1, p2 *OrderData) bool

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

	var orderedList []OrderData
	var itemTable [][]string
	//first := true
	for {
		recordOrd, err := reader.ReadLine()
		if err == io.EOF || recordOrd == "" {
			break
		}
		Error("error in day directory creation", err)
		recordArray := strings.Split(recordOrd, ",")
		if recordArray[1] != "" && recordArray[1] != " " {

			itemTable = append(itemTable, recordArray)
		}

		//if recordArray[1] != "" && recordArray[1] != " " {
		//
		//	if recordArray[0] == "200" {
		//		if !first {
		//
		//			val, err := strconv.ParseInt(recordArray[1], 10, 64)
		//			Error("error in day directory creation", err)
		//			data := OrderData{Nimi: val, Data: itemTable, Status: recordArray[0]}
		//			orderedList = append(orderedList, data)
		//			itemTable=[][]string{}
		//			itemTable = append(itemTable, recordArray)
		//
		//		}else{
		//			itemTable = append(itemTable, recordArray)
		//		}
		//		first = false
		//	}else{
		//		itemTable = append(itemTable, recordArray)
		//	}
		//}
	}

	var owners2 []string
	From(itemTable).Where(func(c interface{}) bool {
		return c.([]string)[0] == "200"
	}).ToSlice(&owners2)

	fmt.Println(len(orderedList))

	sort.Slice(orderedList[:], func(i, j int) bool {

		return orderedList[i].Nimi < orderedList[j].Nimi
	})

	var recordList [][]string
	listChan := make(chan [][]string)
	var nmiFileName string

	//---------------------------------------------
	//group all the object
	//---------------------------------------------
	var dataItems []Group
	From(orderedList).GroupByT(
		func(word OrderData) int64 { return word.Nimi },
		func(word OrderData) [][]string { return word.Data },
	).ToSlice(&dataItems)

	//----------------------------------------------
	//loop to create and spawn go routine
	//----------------------------------------------
	var m sync.RWMutex
	for _, item := range dataItems {

		if len(item.Group) > 0 {
			nmiFileName = fmt.Sprintf("%s%s", "", item.Group[1])
			firstElement := []string{"100", nmiFileName, config.Client, config.Client, "\r\n"}
			recordList = append(recordList, firstElement)
			for _, data := range item.Group {
				recordList = append(recordList, data.([]string))
			}
			recordList = append(recordList, []string{"900", "\r\n"})
			wg.Add(1)
			go ProcessMeterDataSplitting(listChan, &m, &wg, nmiFileName, fileFolderPath, config)
			listChan <- recordList
			wg.Wait()
			recordList = [][]string{}
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
