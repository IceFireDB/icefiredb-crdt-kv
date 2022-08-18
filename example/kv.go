package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	icefiredb_crdt_kv "github.com/IceFireDB/icefiredb-crdt-kv/kv"
	"github.com/sirupsen/logrus"
)

func main() {
	ctx := context.TODO()
	log := logrus.New()
	db, err := icefiredb_crdt_kv.NewCRDTKeyValueDB(ctx, icefiredb_crdt_kv.Config{
		NodeServiceName:     "icefiredb-crdt-kv",
		DataSyncChannel:     "icefiredb-crdt-kv-data",
		NetDiscoveryChannel: "icefiredb-crdt-kv-net",
		Namespace:           "test",
		Logger:              log,
	})
	if err != nil {
		panic(err)
	}

	defer db.Close()

	fmt.Printf("> ")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		fields := strings.Fields(text)
		if len(fields) == 0 {
			fmt.Printf("> ")
			continue
		}

		cmd := fields[0]
		switch cmd {
		case "exit", "quit":
			return
		case "get":
			if len(fields) < 2 {
				printVal("缺少key")
				continue
			}
			val, err := db.Get(ctx, []byte(fields[1]))
			if err != nil {
				printVal(err)
				continue
			}
			printVal(string(val))
		case "put":
			if len(fields) < 3 {
				printVal("缺少参数")
				continue
			}

			printVal(db.Put(ctx, []byte(fields[1]), []byte(fields[2])))
		case "delete":
			if len(fields) < 2 {
				printVal("缺少key")
				continue
			}
			printVal(db.Delete(ctx, []byte(fields[1])))
		case "has":
			if len(fields) < 2 {
				printVal("缺少key")
				continue
			}
			is, err := db.Has(ctx, []byte(fields[1]))
			if err != nil {
				printVal(err)
				continue
			}
			printVal(is)
		case "list":
			result, err := db.Query(ctx)
			if err != nil {
				printVal(err)
				continue
			}
			for val := range result.Next() {
				fmt.Printf(fmt.Sprintf("%s => %v\n", val.Key, string(val.Value)))
			}
			fmt.Print("> ")
		case "connect": // 主动连接
			if len(fields) < 2 {
				printVal("缺少连接地址")
				continue
			}
			err = db.Connect(fields[1])
			if err == nil {
				printVal("连接成功！")
			} else {
				printVal(err)
			}
		default:
			printVal("")
		}
	}
}

func printVal(v interface{}) {
	fmt.Printf("%v\n> ", v)
}
