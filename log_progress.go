package main

import (
	"time"
	"fmt"
	"os"
	"bufio"
	"io"
	"regexp"
	"log"
	"strconv"
	"strings"
	"net/url"
	"github.com/influxdata/influxdb/client/v2"
)

type Reader interface {
	Read(rc chan []byte)
}

type Writer interface {
	Write(wc chan *Message)
}

type logProgress struct {
	rc chan []byte
	wc chan *Message
	read ReadFromFile
	write WriteToInfluxDB
}

type ReadFromFile struct {
	path string // 读取日志文件的路径
}


type WriteToInfluxDB struct {
	influxDBsn string // fluxDB配置信息
}

type Message struct {
	TimeLocal 					 time.Time
	BytesSent  					 int
	Path, Method, Scheme, Status string
	UpstreamTime, RequestTime    float64
}


func (r * ReadFromFile) Read (rc chan []byte) {
	f, err := os.Open(r.path)
	if err != nil {
		panic(fmt.Sprintf("open file error:%s", err.Error()))
	}

	strconv.ParseInt("12", 64, 64)

	f.Seek(0, 2)
	// 从日志文件末尾开始逐行读取内容
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadBytes('\n')
		if err == io.EOF {
			time.Sleep(500*time.Microsecond)
			continue
		} else if err != nil {
			panic(fmt.Sprintln("ReadBytes error:%s", err.Error()))
		}
		rc<-line[:len(line)-1]
	}
}

func (w *WriteToInfluxDB) Write(wc chan *Message)  {
	infSli := strings.Split(w.influxDBsn, "@")

	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     infSli[0],
		Username: infSli[1],
		Password: infSli[2],
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()


	for v := range wc {
		// Create a new point batch
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  infSli[3],
			Precision: infSli[4],
		})
		if err != nil {
			log.Fatal(err)
		}

		// Create a point and add to batch
		tags := map[string]string{"Path": v.Path, "Method": v.Method, "Scheme": v.Scheme, "Status": v.Status}
		fields := map[string]interface{}{
			"UpstreamTime":   v.UpstreamTime,
			"RequestTime": v.RequestTime,
			"BytesSent":   v.BytesSent,
		}

		pt, err := client.NewPoint("nginx_log", tags, fields, v.TimeLocal)
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)

		// Write the batch
		if err := c.Write(bp); err != nil {
			log.Fatal(err)
		}

		// Close client resources
		if err := c.Close(); err != nil {
			log.Fatal(err)
		}
		log.Println("write success!")
	}
}

func (lp *logProgress) Progress() {
	// 解析模块
	/**

	 */
	r := regexp.MustCompile(`([\d\.]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)`)
	loc, _ := time.LoadLocation("Asia/Shanghai")
	for v := range lp.rc {
		ret := r.FindStringSubmatch(string(v))

		if len(ret) !=14 {
			log.Println("FindStringSubmatch fail:", string(3))
			continue
		}
		message := &Message{}
		t, err := time.ParseInLocation("02/Jan/2006:15:04:05 +0000", ret[4], loc)
		if err != nil {
			log.Println("ParseInLocation fail:", err.Error(), ret[4])
		}
		message.TimeLocal = t
		byteSemt, _ := strconv.Atoi(ret[8])
		message.BytesSent = byteSemt

		reqSli := strings.Split(ret[6], " ")
		if len(reqSli) != 3 {
			log.Println("string.Split fail", ret[6])
		}
		message.Method = reqSli[0]
		u, err := url.Parse(reqSli[1])
		if err != nil {
			log.Println("url parse fail", err)
		}
		message.Path = u.Path
		message.Scheme = ret[5]
		message.Status = ret[7]
		upstreamTime, _ := strconv.ParseFloat(ret[12], 64)
		requestTime, _ := strconv.ParseFloat(ret[13], 64)
		message.UpstreamTime = upstreamTime
		message.RequestTime = requestTime
		lp.wc <- message
	}
}

func main() {
	r := ReadFromFile{
		path: "./access.log",
	}
	w := WriteToInfluxDB{
		influxDBsn: "http://127.0.0.1:8086@admin@s799416774@log_analyse@s",
	}
	lp := logProgress{
		rc: make(chan []byte),
		wc: make(chan *Message),
		read: r,
		write: w,
	}
	go lp.read.Read(lp.rc)
	go lp.Progress()
	go lp.write.Write(lp.wc )
	time.Sleep(30 * time.Second)
}
