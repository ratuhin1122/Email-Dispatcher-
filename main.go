package main

type Recipient struct{
	Name string
	Email string
}

func main(){
	loadRecipient("emails.csv")
}
