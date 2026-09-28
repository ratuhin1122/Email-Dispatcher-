package main

import (
	"bytes"
	"html/template"
	"sync"
	"time"
)

type Recipient struct {
	Name  string
	Email string
}

type FailedJob struct {
	Recipient Recipient
	Reason    string
	Error     string
	Timestamp time.Time
}

func main() {

	recipientChan := make(chan Recipient)
	dlqChan := make(chan FailedJob)

	go func() {
		loadRecipient("emails.csv", recipientChan)
	}()

	var workerWg sync.WaitGroup
	workerCount := 3
	for i := 1; i <= workerCount; i++ {
		workerWg.Add(1)
		go emailWorker(i, recipientChan, dlqChan, &workerWg)
	}

	var dlqWg sync.WaitGroup
	dlqWg.Add(1)
	go dlqWorker(dlqChan, &dlqWg)

	// Wait for all email workers to finish
	workerWg.Wait()

	// Close DLQ channel after all email workers are done
	close(dlqChan)

	// Wait for DLQ worker to finish flushing failed emails to disk
	dlqWg.Wait()

}

func executeTemplate(r Recipient) (string, error) {
	t, err := template.ParseFiles("email.tmpl")
	if err != nil {
		return "", err
	}
	var tpl bytes.Buffer
	err = t.Execute(&tpl, r)
	if err != nil {
		return "", err
	}
	return tpl.String(), nil
}
