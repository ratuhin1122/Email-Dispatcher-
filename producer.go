package main

import (
	"encoding/csv"
	"os"
	
)

func loadRecipient(filepath string, recipientChan chan Recipient) error {
	defer close(recipientChan)
	f,err := os.Open(filepath)
	if err != nil {
		return err
	}

	reader := csv.NewReader(f)

	records,err := reader.ReadAll()
	if err != nil {
		return err
	}

	for _, record := range records[1:] {
		// send value to channel
		recipientChan <- Recipient{
			Name: record[0],
			Email: record[1],
		}
		
	}
	return nil
	
}