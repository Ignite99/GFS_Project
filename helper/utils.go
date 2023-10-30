package helper

import (
	"log"

	uuid "github.com/satori/go.uuid"
)

func StringToUUID(input string) uuid.UUID {

	stuff, err := uuid.FromString(input)
	if err != nil {
		log.Println("Error converting string to uuid")
		return uuid.UUID{}
	}

	return stuff
}
