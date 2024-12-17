package main

import (
	"fmt"
	"os"

	_ "net/http/pprof"

	"github.com/JacobHaig/GoFrames/dataframe"
)

func main() {
	df, err := dataframe.ReadCSV("data/addresses.csv", dataframe.Options{
		"delimiter":        ',',
		"trimleadingspace": true,
		"header":           true,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	df3 := df.ApplyIndex("Full Name", func(a ...any) any {
		return a[0].(string) + " " + a[1].(string)
	}, "First Name", "Last Name")

	df3.PrintTable()

	series := df3.GetSeries("Full Name", dataframe.Options{"copy": true})
	df3.DropColumn("Full Name")
	df3.PrintTable()

	for i := range series.Values {
		series.Values[i] = series.Values[i].(string) + "!"
	}

	df3.AddSeries(series)
	df3.PrintTable()
	df3.DropColumn("Full Name")
	df3.PrintTable()

	df5 := df3.FilterIndex(func(a ...any) bool {
		return a[0].(string) != "Tyler"
	}, "Last Name")
	df5.PrintTable()

	df4 := df3.FilterMap(func(m map[string]any) bool {
		return m["First Name"].(string) != "Jack"
	})
	df4.PrintTable()
}

// "github.com/xitongsys/parquet-go-source/local"
// "github.com/xitongsys/parquet-go/common"
// "github.com/xitongsys/parquet-go/reader"
// "github.com/xitongsys/parquet-go/writer"
// type Student struct {
// 	Name   string           `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
// 	Age    int32            `parquet:"name=age, type=INT32"`
// 	Id     int64            `parquet:"name=id, type=INT64"`
// 	Weight float32          `parquet:"name=weight, type=FLOAT"`
// 	Sex    bool             `parquet:"name=sex, type=BOOLEAN"`
// 	Day    int32            `parquet:"name=day, type=INT32, convertedtype=DATE"`
// 	Class  []string         `parquet:"name=class, type=SLICE, convertedtype=SLICE, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
// 	Score  map[string]int32 `parquet:"name=score, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
// }

// func testParquet() {
// 	var err error
// 	//write
// 	fw, err := local.NewLocalFileWriter("column.parquet")
// 	if err != nil {
// 		log.Println("Can't create file", err)
// 		return
// 	}
// 	pw, err :=
// 		writer.NewParquetWriter(fw, new(Student), 4)
// 	if err != nil {
// 		log.Println("Can't create parquet writer")
// 		return
// 	}
// 	num := int64(10)
// 	for i := 0; int64(i) < num; i++ {
// 		stu := Student{
// 			Name:   "StudentName",
// 			Age:    int32(20 + i%5),
// 			Id:     int64(i),
// 			Weight: float32(50.0 + float32(i)*0.1),
// 			Sex:    bool(i%2 == 0),
// 			Day:    int32(time.Now().Unix() / 3600 / 24),
// 			Class:  []string{"Math", "Physics", "Algorithm"},
// 			Score:  map[string]int32{"Math": int32(100 - i), "Physics": int32(100 - i), "Algorithm": int32(100 - i)},
// 		}
// 		if err = pw.Write(stu); err != nil {
// 			log.Println("Write error", err)
// 		}
// 	}
// 	if err = pw.WriteStop(); err != nil {
// 		log.Println("WriteStop error", err)
// 	}
// 	log.Println("Write Finished")
// 	fw.Close()

// 	var names, classes, scores_key, scores_value, ids []any
// 	var rls, dls []int32

// 	///read
// 	fr, err := local.NewLocalFileReader("column.parquet")
// 	if err != nil {
// 		log.Println("Can't open file", err)
// 		return
// 	}
// 	pr, err := reader.NewParquetColumnReader(fr, 4)
// 	if err != nil {
// 		log.Println("Can't create column reader", err)
// 		return
// 	}
// 	num = int64(pr.GetNumRows())

// 	pr.SkipRowsByPath(common.ReformPathStr("parquet_go_root.name"), 5) //skip the first five rows
// 	names, rls, dls, err = pr.ReadColumnByPath(common.ReformPathStr("parquet_go_root.name"), num)
// 	log.Println("name", names, rls, dls, err)

// 	classes, rls, dls, err = pr.ReadColumnByPath(common.ReformPathStr("parquet_go_root.class.list.element"), num)
// 	log.Println("class", classes, rls, dls, err)

// 	scores_key, rls, dls, err = pr.ReadColumnByPath(common.ReformPathStr("parquet_go_root.score.key_value.key"), num)
// 	scores_value, rls, dls, err = pr.ReadColumnByPath(common.ReformPathStr("parquet_go_root.score.key_value.value"), num)
// 	log.Println("parquet_go_root.scores_key", scores_key, err)
// 	log.Println("parquet_go_root.scores_value", scores_value, err)

// 	pr.SkipRowsByIndex(2, 5) //skip the first five rows
// 	ids, _, _, _ = pr.ReadColumnByIndex(2, num)
// 	log.Println(ids)

// 	pr.ReadStop()
// 	fr.Close()
// }

// func main() {

// p := profile.Start(profile.CPUProfile, profile.ProfilePath("."), profile.NoShutdownHook)
// p.Stop()

// Start profiling
// f, _ := os.Create("profile.prof")

// startTime := time.Now()

// pprof.StartCPUProfile(f)
// df, err := dataframe.ReadCSVStandalone("data/output.txt", dataframe.Options{
// 	"delimiter": ';',
// 	// "trimleadingspace": true,
// })
// if err != nil {
// 	fmt.Println(err)
// }
// pprof.StopCPUProfile()

// df.PrintTable()

// elapsedTime := time.Since(startTime)
// fmt.Println("Elapsed time:", elapsedTime)

// time.Sleep(1000 * time.Second)

// }
