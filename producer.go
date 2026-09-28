package main

import (
	"encoding/csv"
	"fmt"
	"os"
	
)

func loadRecipient(filepath string) error {
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
		fmt.Println(record)
		
	}
	return nil
	
}