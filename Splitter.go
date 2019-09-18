package main

import (
	"FileSplitter/splits"
	"github.com/fsnotify/fsnotify"
	"log"
	"time"
)

func main() {

	var processedFile = ""
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
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
						_, err := processor.ProcessCsv(event.Name)
						if err != nil {
							log.Fatal("error in file processing", err)
						}
						processedFile = event.Name
					}
				}
			case err := <-watcher.Errors:
				log.Println("error:", err)
			}
		}
	}()
	err = watcher.Add("D:\\Watcher\\Source")
	if err != nil {
		log.Fatal(err)
	}
	<-done
}
