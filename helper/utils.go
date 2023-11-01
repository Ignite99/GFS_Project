package helper

import (
	"log"
	"fmt"

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

// TODO: handle <10 char string
func TruncateOutput(bytestream []byte) string {
	return fmt.Sprintf("%s... and %d more characters", string(bytestream[:10]), len(bytestream)-10)
}
