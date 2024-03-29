package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	icefiredb_crdt_kv "github.com/IceFireDB/icefiredb-crdt-kv/kv"
	badger2 "github.com/dgraph-io/badger"
	"github.com/ipfs/go-datastore/query"
	"github.com/sirupsen/logrus"
)

func main() {
	ctx := context.TODO()
	log := logrus.New()
	db, err := icefiredb_crdt_kv.NewCRDTKeyValueDB(ctx, icefiredb_crdt_kv.Config{
		NodeServiceName:     "icefiredb-crdt-kv",
		DataSyncChannel:     "icefiredb-crdt-kv-data",
		NetDiscoveryChannel: "icefiredb-crdt-kv-net",
		Namespace:           "icefiredb-crdt-kvdb-ns1",
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
				printVal("miss key")
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
				printVal("Missing parameters")
				continue
			}

			printVal(db.Put(ctx, []byte(fields[1]), []byte(fields[2])))
		case "delete":
			if len(fields) < 2 {
				printVal("miss key")
				continue
			}
			printVal(db.Delete(ctx, []byte(fields[1])))
		case "has":
			if len(fields) < 2 {
				printVal("miss key")
				continue
			}
			is, err := db.Has(ctx, []byte(fields[1]))
			if err != nil {
				printVal(err)
				continue
			}
			printVal(is)
		case "list":
			result, err := db.Query(ctx, query.Query{})
			if err != nil {
				printVal(err)
				continue
			}
			for val := range result.Next() {
				fmt.Printf(fmt.Sprintf("%s => %v\n", val.Key, string(val.Value)))
			}
			fmt.Print("> ")
		case "query":
			if len(fields) < 2 {
				printVal("Missing query conditions")
				continue
			}
			q := query.Query{
				Filters: []query.Filter{
					query.FilterKeyPrefix{
						Prefix: fields[1],
					},
				},
			}
			result, err := db.Query(ctx, q)
			if err != nil {
				printVal(err)
				continue
			}
			for val := range result.Next() {
				fmt.Printf(fmt.Sprintf("%s => %v\n", val.Key, string(val.Value)))
			}
			fmt.Print("> ")
		case "connect":
			if len(fields) < 2 {
				printVal("Missing connection address")
				continue
			}
			err = db.Connect(fields[1])
			if err == nil {
				printVal("connection succeeded!")
			} else {
				printVal(err)
			}
		case "slist":
			result, err := db.Store().Query(ctx, query.Query{})
			if err != nil {
				printVal(err)
				continue
			}
			for val := range result.Next() {
				fmt.Printf(fmt.Sprintf("%s => %v\n", val.Key, string(val.Value)))
			}
			fmt.Print("> ")
		case "bquery":
			if len(fields) < 2 {
				printVal("Missing query conditions")
				continue
			}
			db.DB().View(func(txn *badger2.Txn) error {
				opts := badger2.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				prefix := []byte(fields[1])
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					item := it.Item()
					k := item.Key()
					err := item.Value(func(v []byte) error {
						fmt.Printf("key=%s, value=%s\n", k, v)
						return nil
					})
					if err != nil {
						return err
					}
				}
				return nil
			})

		case "blist":
			db.DB().View(func(txn *badger2.Txn) error {
				opts := badger2.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()
					k := item.Key()
					err := item.Value(func(v []byte) error {
						fmt.Printf("key=%s, value=%s\n", k, v)
						return nil
					})
					if err != nil {
						return err
					}
				}
				return nil
			})
		default:
			printVal("")
		}
	}
}

func printVal(v interface{}) {
	fmt.Printf("%v\n> ", v)
}
