package main

// This is a CLI that lets you join a global permissionless CRDT-based
// database using CRDTs and IPFS.

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/IceFireDB/icefiredb-crdt-kv/pkg/p2p"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	badger "github.com/ipfs/go-ds-badger"
	crdt "github.com/ipfs/go-ds-crdt"
	logging "github.com/ipfs/go-log/v2"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	ipfslite "github.com/hsanjuan/ipfs-lite"

	multiaddr "github.com/multiformats/go-multiaddr"
)

var (
	logger      = logging.Logger("globaldb")
	listen, _   = multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/33124")
	topicName   = "globaldb-example"
	netTopic    = "globaldb-example-net"
	serviceName = "icefiredb-crdt-kv3"
	config      = "globaldb-example"
)

var p2phost *p2p.P2P

func main() {
	// Bootstrappers are using 1024 keys. See:
	// https://github.com/ipfs/infra/issues/378
	crypto.MinRsaKeyBits = 1024

	logging.SetLogLevel("*", "error")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := os.Getwd()
	if err != nil {
		logger.Fatal(err)
	}
	data := filepath.Join(dir, config)

	store, err := badger.NewDatastore(data, &badger.DefaultOptions)
	if err != nil {
		logger.Fatal(err)
	}
	defer store.Close()

	keyPath := filepath.Join(data, "key")
	var priv crypto.PrivKey
	_, err = os.Stat(keyPath)
	if os.IsNotExist(err) {
		priv, _, err = crypto.GenerateKeyPair(crypto.Ed25519, 1)
		if err != nil {
			logger.Fatal(err)
		}
		data, err := crypto.MarshalPrivateKey(priv)
		if err != nil {
			logger.Fatal(err)
		}
		err = ioutil.WriteFile(keyPath, data, 0400)
		if err != nil {
			logger.Fatal(err)
		}
	} else if err != nil {
		logger.Fatal(err)
	} else {
		key, err := ioutil.ReadFile(keyPath)
		if err != nil {
			logger.Fatal(err)
		}
		priv, err = crypto.UnmarshalPrivateKey(key)
		if err != nil {
			logger.Fatal(err)
		}

	}
	// pid, err := peer.IDFromPublicKey(priv.GetPublic())
	// if err != nil {
	// 	logger.Fatal(err)
	// }

	p2phost = p2p.NewP2P(serviceName) // create p2p
	log.Println("host peer id: ", p2phost.Host.ID())
	p2phost.AdvertiseConnect()
	log.Println("Connected to P2P Service Peers")

	// tlstransport, err := tls.New(priv)
	// if err != nil {
	// 	log.Fatalln(err)
	// }
	// security := libp2p.Security(tls.ID, tlstransport)
	// transport := libp2p.Transport(tcp.NewTCPTransport)

	// // Set up host listener address options
	// muladdr, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/0")
	// listen := libp2p.ListenAddrs(muladdr)

	// if err != nil {
	// 	log.Fatalln(err)
	// }

	// log.Println("Generated P2P Address Listener Configuration.")

	// // Set up the stream multiplexer and connection manager options
	// muxer := libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport)
	// conn := libp2p.ConnectionManager(connmgr.NewConnManager(100, 400, time.Minute))

	// // Setup NAT traversal and relay options
	// nat := libp2p.NATPortMap()
	// relay := libp2p.EnableAutoRelay()

	// // Declare a KadDHT
	// var kaddht *dht.IpfsDHT
	// // Setup a routing configuration with the KadDHT
	// //定义节点路由函数,设置节点发现函数
	// routing := libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
	// 	kaddht = setupKadDHT(ctx, h)
	// 	return kaddht, err
	// })

	// identity := libp2p.Identity(priv)
	// opts := libp2p.ChainOptions(identity, listen, security, transport, muxer, conn, nat, routing, relay)

	// // Construct a new libP2P host with the created options
	// libhost, err := libp2p.New( opts)

	// if err != nil {
	// 	log.Fatalln(err)
	// }

	// h, dht, err := ipfslite.SetupLibp2p(
	// 	ctx,
	// 	priv,
	// 	nil,
	// 	[]multiaddr.Multiaddr{listen},
	// 	nil,
	// 	ipfslite.Libp2pOptionsExtra...,
	// )
	// if err != nil {
	// 	logger.Fatal(err)
	// }
	// defer h.Close()
	// defer dht.Close()

	psub := p2phost.PubSub

	topic, err := psub.Join(netTopic)
	if err != nil {
		logger.Fatal(err)
	}

	netSubs, err := topic.Subscribe()
	if err != nil {
		logger.Fatal(err)
	}

	// Use a special pubsub topic to avoid disconnecting
	// from globaldb peers.
	go func() {
		for {
			msg, err := netSubs.Next(ctx)
			if err != nil {
				fmt.Println(err)
				break
			}
			p2phost.Host.ConnManager().TagPeer(msg.ReceivedFrom, "keep", 100)
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				topic.Publish(ctx, []byte("hi!"))
				time.Sleep(20 * time.Second)
			}
		}
	}()

	ipfs, err := ipfslite.New(ctx, store, p2phost.Host, p2phost.KadDHT, nil)
	if err != nil {
		logger.Fatal(err)
	}

	pubsubBC, err := crdt.NewPubSubBroadcaster(ctx, psub, topicName)
	if err != nil {
		logger.Fatal(err)
	}

	opts := crdt.DefaultOptions()
	opts.Logger = logger
	opts.RebroadcastInterval = 5 * time.Second
	opts.PutHook = func(k ds.Key, v []byte) {
		fmt.Printf("Added: [%s] -> %s\n", k, string(v))

	}
	opts.DeleteHook = func(k ds.Key) {
		fmt.Printf("Removed: [%s]\n", k)
	}

	crdt, err := crdt.New(store, ds.NewKey("crdt"), ipfs, pubsubBC, opts)
	if err != nil {
		logger.Fatal(err)
	}
	defer crdt.Close()

	fmt.Println("Bootstrapping...")

	bstr, _ := multiaddr.NewMultiaddr("/ip4/94.130.135.167/tcp/33123/ipfs/12D3KooWFta2AE7oiK1ioqjVAKajUJauZWfeM7R413K7ARtHRDAu")
	inf, _ := peer.AddrInfoFromP2pAddr(bstr)
	list := append(ipfslite.DefaultBootstrapPeers(), *inf)
	ipfs.Bootstrap(list)
	p2phost.Host.ConnManager().TagPeer(inf.ID, "keep", 100)

	fmt.Printf(`
Peer ID: %s
Listen address: %s
Topic: %s
Data Folder: %s

Ready!

Commands:

> list               -> list items in the store
> get <key>          -> get value for a key
> put <key> <value>  -> store value on a key
> exit               -> quit


`,
		p2phost.Host.ID(), listen, topicName, data,
	)

	if len(os.Args) > 1 && os.Args[1] == "daemon" {
		fmt.Println("Running in daemon mode")
		go func() {
			for {
				fmt.Printf("%s - %d connected peers\n", time.Now().Format(time.Stamp), len(connectedPeers(p2phost.Host)))
				time.Sleep(10 * time.Second)
			}
		}()
		signalChan := make(chan os.Signal, 20)
		signal.Notify(
			signalChan,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGHUP,
		)
		<-signalChan
		return
	}

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
		case "debug":
			if len(fields) < 2 {
				fmt.Println("debug <on/off/peers>")
			}
			st := fields[1]
			switch st {
			case "on":
				logging.SetLogLevel("globaldb", "debug")
			case "off":
				logging.SetLogLevel("globaldb", "error")
			case "peers":
				for _, p := range connectedPeers(p2phost.Host) {
					addrs, err := peer.AddrInfoToP2pAddrs(p)
					if err != nil {
						logger.Warn(err)
						continue
					}
					for _, a := range addrs {
						fmt.Println(a)
					}
				}
			}
		case "list":
			q := query.Query{}
			results, err := crdt.Query(ctx, q)
			if err != nil {
				printErr(err)
			}
			for r := range results.Next() {
				if r.Error != nil {
					printErr(err)
					continue
				}
				fmt.Printf("[%s] -> %s\n", r.Key, string(r.Value))
			}
		case "get":
			if len(fields) < 2 {
				fmt.Println("get <key>")
				fmt.Println("> ")
				continue
			}
			k := ds.NewKey(fields[1])
			v, err := crdt.Get(ctx, k)
			if err != nil {
				printErr(err)
				continue
			}
			fmt.Printf("[%s] -> %s\n", k, string(v))
		case "put":
			if len(fields) < 3 {
				fmt.Println("put <key> <value>")
				fmt.Println("> ")
				continue
			}
			k := ds.NewKey(fields[1])
			v := strings.Join(fields[2:], " ")
			err := crdt.Put(ctx, k, []byte(v))
			if err != nil {
				printErr(err)
				continue
			}
		}
		fmt.Printf("> ")
	}
}

func printErr(err error) {
	fmt.Println("error:", err)
	fmt.Println("> ")
}

func connectedPeers(h host.Host) []*peer.AddrInfo {
	var pinfos []*peer.AddrInfo
	for _, c := range h.Network().Conns() {
		pinfos = append(pinfos, &peer.AddrInfo{
			ID:    c.RemotePeer(),
			Addrs: []multiaddr.Multiaddr{c.RemoteMultiaddr()},
		})
	}
	return pinfos
}
