package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type myStruct struct {
	BaseBbl    int64   `json:"base_bbl,string"`
	Bin        int64   `json:"bin,string"`
	CnstrctYr  int64   `json:"cnstrct_yr,string"`
	DoittID    int64   `json:"doitt_id,string"`
	FeatCode   int64   `json:"feat_code,string"`
	Geomsource string  `json:"geomsource,omitempty"`
	Groundelev int64   `json:"groundelev,string"`
	Heightroof float64 `json:"heightroof,string"`
	Lstmoddate string  `json:"lstmoddate,omitempty"`
	Lststatype string  `json:"lststatype,omitempty"`
	MplutoBbl  int64   `json:"mpluto_bbl,string"`
	ShapeArea  float64 `json:"shape_area,string"`
	ShapeLen   float64 `json:"shape_len,string"`
	// TheGeom    struct {
	// 	Type        string          `json:"type,omitempty"`
	// 	Coordinates [][][][]float64 `json:"coordinates,omitempty"`
	// } `json:"the_geom,omitempty"`
}

var client1 *mongo.Client

func initializeDb() { //Initializing MongoDB database
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

	client1, _ = mongo.Connect(ctx, clientOptions)
}

func add(as []myStruct) { //Adding dataset to mongo database
	collection := client1.Database("LocalDatabase").Collection("Entries")
	ctx1, _ := context.WithTimeout(context.Background(), 5*time.Second)
	for i, v := range as {
		fmt.Println(i, v)
		collection.InsertOne(ctx1, v)
	}
}
func SumOrAvgEndpoint(response http.ResponseWriter, request *http.Request) {
	params := mux.Vars(request)
	op, _ := params["operation"]
	field, _ := params["field"]

	collection := client1.Database("LocalDatabase").Collection("Entries")
	ctx1, _ := context.WithTimeout(context.Background(), 5*time.Second)

	filter2 := mongo.Pipeline{{{"$group", bson.D{{"_id", "null"}, {"totalPop", bson.D{{"$" + op, "$" + field}}}}}}}
	cursor, err := collection.Aggregate(ctx1, filter2, options.Aggregate())

	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(`{"message": "` + err.Error() + `"}"`))
		return
	}

	defer cursor.Close(ctx1)

	for cursor.Next(ctx1) {

		fmt.Fprintf(response, "%s", cursor.Current.Lookup("totalPop"))

	}
}
func GetGreaterOrSmallerEndpoint(response http.ResponseWriter, request *http.Request) {
	response.Header().Add("content-type", "application/json")

	params := mux.Vars(request)
	op, _ := params["op"]
	number, _ := params["number"]
	field, _ := params["field"]
	var number1 int
	number1, _ = strconv.Atoi(number)
	collection := client1.Database("LocalDatabase").Collection("Entries")
	ctx1, _ := context.WithTimeout(context.Background(), 5*time.Second)
	var arr []myStruct
	filter := bson.M{field: bson.M{"$" + op: number1}}
	cursor, err := collection.Find(ctx1, filter)
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(`{"message": "` + err.Error() + `"}"`))
		return
	}
	defer cursor.Close(ctx1)
	for cursor.Next(ctx1) {
		var entry myStruct
		cursor.Decode(&entry)
		arr = append(arr, entry)

	}
	if err := cursor.Err(); err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(`{"message": "` + err.Error() + `"}"`))
		return
	}

	json.NewEncoder(response).Encode(arr)

}
func SearchField(response http.ResponseWriter, request *http.Request) {
	params := mux.Vars(request)
	field, _ := params["field"]
	value, _ := params["value"]
	filter := bson.M{field: value}
	collection := client1.Database("LocalDatabase").Collection("Entries")
	ctx1, _ := context.WithTimeout(context.Background(), 5*time.Second)
	if field == "geomsource" || field == "lstmoddate" || field == "lststatype" {
		filter = bson.M{field: value}
	} else if field == "shapelen" || field == "shapearea" || field == "heightroof" {
		var value2 float64
		value2, _ = strconv.ParseFloat(value, 64)
		filter = bson.M{field: value2}

	} else {
		var value1 int
		value1, _ = strconv.Atoi(value)
		filter = bson.M{field: value1}
	}

	cursor, err := collection.Find(ctx1, filter)
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(`{"message": "` + err.Error() + `"}"`))
		return
	}
	defer cursor.Close(ctx1)
	var arr []myStruct
	for cursor.Next(ctx1) {
		var entry myStruct
		cursor.Decode(&entry)
		arr = append(arr, entry)

	}
	json.NewEncoder(response).Encode(arr)

}
func homeHandler(response http.ResponseWriter, request *http.Request) {
	fmt.Fprint(response, "INSTRUCTIONS:\n\n")
	fmt.Fprint(response, "There are three main query types:\n\n")
	fmt.Fprint(response, "1.Getting the average or sum of one field of dataset\n")
	fmt.Fprint(response, "2.Comparing a field of every row with a given number and list the results of the operation which is greater than or less than\n")
	fmt.Fprint(response, "3.Search for a specific key : value\n\n")
	fmt.Fprint(response, "EXAMPLE USAGE FOR COMPARE\n")
	fmt.Fprint(response, "\n")
	fmt.Fprint(response, " 	http://localhost:4444/search/geomsource-Photogramm\n")
	fmt.Fprint(response, " 	http://localhost:4444/compare/bin-lt-2000000\n\n")
	fmt.Fprint(response, "EXAMPLE USAGE FOR SEARCH\n")
	fmt.Fprint(response, "\n")
	fmt.Fprint(response, " 	http://localhost:4444/compare/cnstrctyr-gt-2015\n")
	fmt.Fprint(response, " 	http://localhost:4444/search/heightroof-76.93\n")
	fmt.Fprint(response, " 	http://localhost:4444/search/cnstrctyr-2013\n")
	fmt.Fprint(response, "\n\n")
	fmt.Fprint(response, "EXAMPLE USAGE FOR GETTING AVERAGE AND SUM OF VALUES\n")
	fmt.Fprint(response, "\n\n")
	fmt.Fprint(response, " 	http://localhost:4444/op/avg-bin\n")
	fmt.Fprint(response, " 	http://localhost:4444/op/sum-cnstrctyr\n")
	fmt.Fprint(response, "\n\n")
	fmt.Fprint(response, "Do not use underscore while making a query:\n")
	fmt.Fprint(response, "\n\n")
	fmt.Fprint(response, "Fields:\n\tbase_bbl\n\tbin\n\tcnstrct_yr\n\tdoitt_id\n\tfeat_code\n\tgeomsource\n\tgroundelev\n\theightroof\n\tlstmoddate\n\tlststatype\n\tmpluto_bbl\n\tshape_area\n\n")
}
func main() {
	response, err := http.Get("https://data.cityofnewyork.us/resource/mtik-6c5q.json") //Fetch dataset

	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}

	responseData, err := ioutil.ReadAll(response.Body)

	if err != nil {
		log.Fatal(err)
	}
	initializeDb()
	xp := []myStruct{}
	fmt.Println(xp)
	json.Unmarshal(responseData, &xp)
	add(xp)
	router := mux.NewRouter()
	router.HandleFunc("/", homeHandler).Methods("GET")
	router.HandleFunc("/op/{operation}-{field}", SumOrAvgEndpoint).Methods("GET")
	router.HandleFunc("/compare/{field}-{op}-{number}", GetGreaterOrSmallerEndpoint).Methods("GET")
	router.HandleFunc("/search/{field}-{value}", SearchField).Methods("GET")
	log.Fatal(http.ListenAndServe(":4444", router))

}
