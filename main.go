package main
import (
    "encoding/json"
    "fmt"
    "github.com/garyburd/redigo/redis"
    "log"
    "strconv"
    "time"
)
type redisSample struct {
    Field1   string `json:  "field1"`
    Field2   string `json:  "field2"`
    Field3   string `json:  "field3"`
    Field4   string `json:  "field4"`
    Field5   string `json:  "field5"`
    Field6   string `json:  "field6"`
    Field7   string `json:  "field7"`
    Field8   string `json:  "field8"`
    Field9   string `json:  "field9"`
    Field10  string `json: "field10"`
}

func InitSetJson(dbindex int,c redis.Conn){
    c.Do("SELECT", dbindex)
    startTime := time.Now()
    for i :=0;i < 1000000; i++{
        sample := redisSample{
            strconv.Itoa(i),
            strconv.Itoa(i),
            strconv.Itoa(i),
            strconv.Itoa(i),
            strconv.Itoa(i),
            strconv.Itoa(i),
            strconv.Itoa(i),
            strconv.Itoa(i),
            strconv.Itoa(i),
            strconv.Itoa(i),
        }
        redisJson,err := json.Marshal(sample)

        if err != nil {
            panic(err)
        }
        stringJson := string(redisJson)
        c.Send("SET","hogehoghogehogehoge"+strconv.Itoa(i),stringJson)
    }
    c.Flush()
    endTime := time.Now()
    InitSetJsonTime := endTime.Sub(startTime).Seconds()
    fmt.Print("InitSetJsonTime is ")
    fmt.Println(InitSetJsonTime)
}

func InitSetList(dbindex int,c redis.Conn){
    c.Do("SELECT",dbindex)
    startTime := time.Now()
    for i :=0; i < 1000000; i++{
        for j :=0; j < 10; j++ {
            c.Send("rpush", "hogehoghogehogehoge"+strconv.Itoa(i), strconv.Itoa(j))
        }
    }
    c.Flush()
    endTime := time.Now()
    InitSetListTime := endTime.Sub(startTime).Seconds()
    fmt.Print("InitSetListTime is ")
    fmt.Println(InitSetListTime)
}

func InitSetHash(dbindex int,c redis.Conn){
    c.Do("SELECT",dbindex)
    startTime := time.Now()
    for i :=0; i < 1000000; i++{
        c.Send("hmset","hogehoghogehogehoge"+strconv.Itoa(i), "Field1", strconv.Itoa(i),
            "Field2", strconv.Itoa(i), "Field3", strconv.Itoa(i), "Field4", strconv.Itoa(i),
            "Field5", strconv.Itoa(i), "Field6", strconv.Itoa(i), "Field7", strconv.Itoa(i),
            "Field8", strconv.Itoa(i), "Field9", strconv.Itoa(i), "Field10", strconv.Itoa(i))
    }
    c.Flush()
    endTime := time.Now()
    initSetHashTime := endTime.Sub(startTime).Seconds()
    fmt.Print("InitSetHashTime is ")
    fmt.Println(initSetHashTime)
}

func getJson(key string, c redis.Conn) {
    c.Do("SELECT", 0)
    startTime := time.Now()
    redisJson,err:= redis.Bytes(c.Do("GET",key))
    if err != nil{
        panic(err)
    }
    var redisSample redisSample
    err = json.Unmarshal(redisJson,&redisSample)
    if err != nil{
        panic(err)
    }
    endTime := time.Now()
    getJsonTime := endTime.Sub(startTime).Seconds()
    //fmt.Print(redisSample)
    fmt.Print("getJsonTime is ")
    fmt.Println(getJsonTime)
}

func setJson(key string, value redisSample, c redis.Conn){
    c.Do("SELECT", 0)
    startTime := time.Now()
    redisJson,err := json.Marshal(value)
    if err != nil{
        panic(err)
    }
    c.Do("Set",key,string(redisJson))
    endTime := time.Now()
    setListTime := endTime.Sub(startTime).Seconds()
    fmt.Print("setJsonTime is ")
    fmt.Println(setListTime)
}

func updateJson(key string, value redisSample, c redis.Conn){
    c.Do("SELECT", 0)
    startTime := time.Now()
    redisJson,err := json.Marshal(value)
    if err != nil{
        panic(err)
    }
    c.Do("Set",key,string(redisJson))
    endTime := time.Now()
    updateJsonTime := endTime.Sub(startTime).Seconds()
    fmt.Print("updateJsonTime is ")
    fmt.Println(updateJsonTime)
}

func deleteJson(key string,  c redis.Conn){
    c.Do("SELECT", 0)
    startTime := time.Now()
    c.Do("del",key)
    endTime := time.Now()
    deleteJsonTime := endTime.Sub(startTime).Seconds()
    fmt.Print("deleteJsonTime is ")
    fmt.Println(deleteJsonTime)
}

func getList(key string, c redis.Conn){
    c.Do("SELECT", 1)
    startTime := time.Now()
    redis.ByteSlices(c.Do("Lrange",key,0,9))
    endTime := time.Now()
    getListTime := endTime.Sub(startTime).Seconds()
    //fmt.Println(list)
    fmt.Print("getListTime is ")
    fmt.Println(getListTime)
}

func setList(key string, value redisSample, c redis.Conn){
    c.Do("SELECT", 1)
    startTime := time.Now()
    c.Send("rpush",key,value.Field1)
    c.Send("rpush",key,value.Field2)
    c.Send("rpush",key,value.Field3)
    c.Send("rpush",key,value.Field4)
    c.Send("rpush",key,value.Field5)
    c.Send("rpush",key,value.Field6)
    c.Send("rpush",key,value.Field7)
    c.Send("rpush",key,value.Field8)
    c.Send("rpush",key,value.Field9)
    c.Send("rpush",key,value.Field10)
    c.Flush()
    endTime := time.Now()
    setListTime := endTime.Sub(startTime).Seconds()
    fmt.Print("setListTime is ")
    fmt.Println(setListTime)
}

func updateList(key string, value redisSample, c redis.Conn){
    c.Do("SELECT", 1)
    startTime := time.Now()
    c.Send("lset",key,9,value.Field1)
    c.Send("lset",key,8,value.Field2)
    c.Send("lset",key,7,value.Field3)
    c.Send("lset",key,6,value.Field4)
    c.Send("lset",key,5,value.Field5)
    c.Send("lset",key,4,value.Field6)
    c.Send("lset",key,3,value.Field7)
    c.Send("lset",key,2,value.Field8)
    c.Send("lset",key,1,value.Field9)
    c.Send("lset",key,0,value.Field10)
    c.Flush()
    endTime := time.Now()
    updateListTime := endTime.Sub(startTime).Seconds()
    fmt.Print("updateListTime is ")
    fmt.Println(updateListTime)
}

func deleteList(key string, c redis.Conn){
    c.Do("SELECT", 1)
    startTime := time.Now()
    c.Do("del",key)
    endTime := time.Now()
    deleteListTime := endTime.Sub(startTime).Seconds()
    fmt.Print("deleteListTime is ")
    fmt.Println(deleteListTime)
}

func getHash(key string, c redis.Conn){
    c.Do("SELECT", 2)
    startTime := time.Now()
    hashStruct := new(redisSample)
    values ,err := redis.Values(c.Do("hmget",key,"Field1","Field2",
        "Field3","Field4","Field5", "Field6","Field7","Field8", "Field9","Field10"));
    if err !=nil{
            log.Fatal(err)
    }
    _, err = redis.Scan(values,&hashStruct.Field1,&hashStruct.Field2,
        &hashStruct.Field3,&hashStruct.Field4,&hashStruct.Field5,&hashStruct.Field6,
        &hashStruct.Field7, &hashStruct.Field8,&hashStruct.Field9,&hashStruct.Field10);
    if err != nil{
        log.Fatal(err)
    }
    endTime := time.Now()
    getHashTime := endTime.Sub(startTime).Seconds()
    fmt.Print("getHashTime is ")
    fmt.Println(getHashTime)
}

func setHash(key string, value redisSample, c redis.Conn){
    c.Do("SELECT", 2)
    startTime := time.Now()
    c.Do("hmset",key, "Field1", value.Field1,
        "Field2", value.Field2, "Field3", value.Field3, "Field4", value.Field4,
        "Field5", value.Field5, "Field6", value.Field6, "Field7", value.Field7,
        "Field8", value.Field8, "Field9", value.Field9, "Field10", value.Field10)
    endTime := time.Now()
    setHashTime := endTime.Sub(startTime).Seconds()
    fmt.Print("setHashTime is ")
    fmt.Println(setHashTime)
}

func updateHash(key string, value redisSample, c redis.Conn){
    c.Do("SELECT", 2)
    startTime := time.Now()
    c.Do("hmset",key, "Field1", value.Field1,
        "Field2", value.Field2, "Field3", value.Field3, "Field4", value.Field4,
        "Field5", value.Field5, "Field6", value.Field6, "Field7", value.Field7,
        "Field8", value.Field8, "Field9", value.Field9, "Field10", value.Field10)
    endTime := time.Now()
    updateHashTime := endTime.Sub(startTime).Seconds()
    fmt.Print("updateHashTime is ")
    fmt.Println(updateHashTime)
}

func deleteHash(key string, c redis.Conn){
    c.Do("SELECT", 2)
    startTime := time.Now()
    c.Do("del",key)
    endTime := time.Now()
    deleteHashTime := endTime.Sub(startTime).Seconds()
    fmt.Print("deleteHashTime is ")
    fmt.Println(deleteHashTime)
}

func redis_connection() redis.Conn {
    const IP_PORT = "localhost:6379"
    //redisに接続
    c, err := redis.Dial("tcp", IP_PORT)
    if err != nil {
        panic(err)
    }
    return c
}

func flashDB(dbindex int, c redis.Conn){
    c.Do("select",dbindex)
    startTime := time.Now()
    c.Do("flushdb")
    endTime := time.Now()
    flushDBTime := endTime.Sub(startTime).Seconds()
    var dataFormat string
    switch(dbindex){
    case 0:
        dataFormat = "Json"
        fmt.Print("flushDB"+dataFormat+"Time is ")
        fmt.Println(flushDBTime)
        return
    case 1:
        dataFormat = "List"
        fmt.Print("flushDB"+dataFormat+"Time is ")
        fmt.Println(flushDBTime)
        return
    case 2:
        dataFormat = "hash"
        fmt.Print("flushDB"+dataFormat+"Time is ")
        fmt.Println(flushDBTime)
        return
    }
}

func main() {
    c := redis_connection()
    defer c.Close()
    defer flashDB(2,c)
    defer flashDB(1,c)
    defer flashDB(0,c)
    sample := redisSample{
        "hogehogehogehoge",
        "1",
        "1",
        "1",
        "1",
        "1",
        "13",
        "3",
        "3",
        "3",
    }
    InitSetJson(0,c)
    InitSetList(1,c)
    InitSetHash(2,c)
    getJson("hogehoghogehogehoge99" , c)
    setJson("hogehoghogehogehoge1000001",sample,c)
    updateJson("hogehoghogehogehoge99",sample,c)
    deleteJson("hogehoghogehogehoge99",c)
    getList("hogehoghogehogehoge99",c)
    setList("hogehoghogehogehoge1000001",sample,c)
    updateList("hogehoghogehogehoge99",sample,c)
    deleteList("hogehoghogehogehoge99",c)
    getHash("hogehoghogehogehoge99", c)
    setHash("hogehoghogehogehoge1000001",sample,c)
    updateHash("hogehoghogehogehoge99",sample,c)
    deleteHash("hogehoghogehogehoge99",c)
}

