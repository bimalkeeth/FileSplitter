package main

import (
	"FileSplitter/splits"
	"encoding/json"
	"github.com/fsnotify/fsnotify"
	"io/ioutil"
	"log"
	"time"
)

func main() {
	defer splits.Panicking()
	var processedFile = ""
	data, err := ioutil.ReadFile("config.json")
	splits.Error("error in confile file reading", err)

	config := &splits.Config{}
	err = json.Unmarshal(data, config)

	watcher, err := fsnotify.NewWatcher()
	splits.Error("watcher error", err)

	defer watcher.Close()
	done := make(chan bool)
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				if event.Op == fsnotify.Write {
					time.Sleep(1000 * time.Millisecond)

					if processedFile != event.Name {

						log.Println("Modified file", event.Name)
						processor := splits.New()
						destPath, err := processor.ProcessCsv(event.Name, config)
						splits.Error("error in file processing", err)
						splits.MoveFile(event.Name, destPath)
						processedFile = event.Name

					}
				}
			case err := <-watcher.Errors:
				log.Println("error:", err)
			}
		}
	}()
	err = watcher.Add(config.Source)
	splits.Error("error in adding file to watcher", err)
	<-done
}
