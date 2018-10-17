package influxclient

import (
	"encoding/json"
	"fmt"
	"time"
	
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	"github.com/influxdata/influxdb/client/v2"
)

// log is the default package logger which we'll use to log
var logg = logger.GetLogger("activity-influxclient")

// MyActivity is a stub for your Activity implementation
type MyActivity struct {
	metadata *activity.Metadata
}

// NewActivity creates a new activity
func NewActivity(metadata *activity.Metadata) activity.Activity {
	return &MyActivity{metadata: metadata}
}

// Metadata implements activity.Activity.Metadata
func (a *MyActivity) Metadata() *activity.Metadata {
	return a.metadata
}

// Eval implements activity.Activity.Eval
func (a *MyActivity) Eval(context activity.Context) (done bool, err error) {

	// do eval
	message := context.GetInput("data").(string)
	database := context.GetInput("database").(string)
	server_address := context.GetInput("addr").(string)

	logg.Debugf("Test connection db [%s] to [%s]", database, server_address)
	
	//Convert the json to influx fields
	byt := []byte(message)

	var fields map[string]interface{}
	
	if err := json.Unmarshal(byt, &fields); err != nil {
		context.SetOutput("result", "ERROR_JSON_DECODE")
		return true, nil
	}

	// Make client
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: server_address,
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}
	defer c.Close()

	// Create a new point batch
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  database,
		Precision: "s",
	})

	// Create a point and add to batch
	tags := map[string]string{"test": "test-message"}
	pt, err := client.NewPoint("test_msg", tags, fields, time.Now())
	if err != nil {
		fmt.Println("Error: ", err.Error())
	}
	bp.AddPoint(pt)

	// Write the batch
	c.Write(bp)

	context.SetOutput("output", "Successfully Inserted "+database+" to "+server_address)

	return true, nil
}
