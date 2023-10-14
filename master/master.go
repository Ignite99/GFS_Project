package main

import (
	"log"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/sutd_gfs_project/helper"
)

func RegisterRoutes(r *gin.Engine) {

}

func main() {
	logfile, err := os.OpenFile("./logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Error opening log file:", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	r := gin.Default()
	RegisterRoutes(r)

	r.Run(":" + strconv.Itoa(helper.MASTER_SERVER_PORT))
}
