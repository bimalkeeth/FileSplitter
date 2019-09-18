package splits

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"trimmer.io/go-csv"
)
type ICsvProcessor interface {
	ProcessCsv(filePath string)(bool,error)
}

type CsvProcess struct {}

func New() ICsvProcessor {
	return &CsvProcess{}
}

type GenericRecord struct {
	Record map[string]string `csv:,any`
}

type GenericCSV []GenericRecord


func(p *CsvProcess)ProcessCsv(filePath string)(bool,error){

	wg := sync.WaitGroup{}
    file,err:=os.Open(filePath)

    defer file.Close()
	if err!=nil{
		return false,err
	}
    reader:=csv.NewDecoder(bufio.NewReader(file)).SkipUnknown(true)

    var recordList =make([][]string,0)
    firstRecord:=true
    listChan:=make(chan  [][]string)

    for{

    	var itemList []string
    	record,err:=reader.ReadLine()
    	if err==io.EOF{
    		break
		}
    	if err!=nil {
    		log.Fatal("error in processing csv file")
		}
    	if record==""{
    		break
		}
        recordArray:=strings.Split(record,",")
        if recordArray[0]=="200"{
          if !firstRecord{

			 var m sync.RWMutex
			 finalRow:=make([]string, 0)
			 finalRow =append(finalRow,"900")
			 recordList=append(recordList,finalRow)
			 go ProcessMeterDataSplitting(listChan,&m,&wg)

			 listChan <- recordList
			 wg.Add(1)
			 wg.Wait()
			 recordList =make([][]string,0)

		  }
         // recLength:=len(recordArray)
		  firstList := make([]string, 0)

		  firstList =append(firstList,"100")
		  firstList =append(firstList, fmt.Sprintf("%s%s","NMI",recordArray[1]) )
		  firstList =append(firstList,"ORIGIN")
		  firstList =append(firstList,"ORIGIN")


		  recordList=append(recordList,firstList)
          for _,item :=range  recordArray{
			  itemList=append(itemList,item)
           }
		  recordList=append(recordList,itemList)
		  firstRecord=false
		}else{

			recordList=append(recordList,recordArray)
		}

		//fmt.Println(record)
	}
	return true,nil
}

func ProcessMeterDataSplitting(arr <-chan [][]string , m *sync.RWMutex, wg *sync.WaitGroup){

	m.Lock()
	select {
	case val:=<-arr:
		for _,item :=range val{
			fmt.Println(item)
		}
	}
	m.Unlock()
	wg.Done()

}




