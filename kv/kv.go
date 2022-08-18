package kv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/IceFireDB/icefiredb-crdt-kv/pkg/p2p"
	ipfslite "github.com/hsanjuan/ipfs-lite"
	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	badger "github.com/ipfs/go-ds-badger"
	crdt "github.com/ipfs/go-ds-crdt"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	ma "github.com/multiformats/go-multiaddr"
)

type Config struct {
	NodeServiceName     string // Service Discovery Identification
	DataStorePath       string // Data storage path
	DataSyncChannel     string // Pubsub data synchronization channel
	NetDiscoveryChannel string // Node discovery channel
	PrivateKey          []byte // As the private key
	Namespace           string
	ListenPort          string
	Logger              logging.StandardLogger
}

type CRDTKeyValueDB struct {
	cfg        *Config
	privateKey crypto.PrivKey
	store      *badger.Datastore
	p2p        *p2p.P2P
	crdt       *crdt.Datastore
}

func NewCRDTKeyValueDB(ctx context.Context, c Config) (*CRDTKeyValueDB, error) {
	if len(c.NodeServiceName) == 0 {
		return nil, errors.New("config NodeServiceName error")
	}
	if len(c.DataSyncChannel) == 0 {
		return nil, errors.New("config DataSyncChannel error")
	}
	if len(c.NetDiscoveryChannel) == 0 {
		return nil, errors.New("config NetDiscoveryChannel error")
	}
	if len(c.DataStorePath) == 0 {
		c.DataStorePath = "./crdtkvdb"
	}
	if len(c.ListenPort) == 0 {
		c.ListenPort = "0"
	}

	db := CRDTKeyValueDB{cfg: &c}
	var err error

	db.store, err = badger.NewDatastore(c.DataStorePath, &badger.DefaultOptions)
	if err != nil {
		return nil, err
	}

	if len(c.PrivateKey) > 0 {
		db.privateKey, err = crypto.UnmarshalPrivateKey(c.PrivateKey)
		if err != nil {
			return nil, err
		}
	} else {
		fileName := filepath.Join(c.DataStorePath, "privatekey")
		if IsFileExist(fileName) {
			if data, err := os.ReadFile(fileName); err == nil {
				db.privateKey, _ = crypto.UnmarshalPrivateKey(data)
			}
		}
		if db.privateKey == nil {
			db.privateKey, _, err = crypto.GenerateKeyPair(crypto.ECDSA, 1)
			if err != nil {
				return nil, err
			}
			// store
			if data, err := crypto.MarshalPrivateKey(db.privateKey); err == nil {
				_ = os.WriteFile(fileName, data, 0600)
			}
		}
	}

	// init p2p
	db.p2p = p2p.NewP2P(c.NodeServiceName, db.privateKey, c.ListenPort)
	db.p2p.AdvertiseConnect()
	if err := db.nodeNetPubSub(ctx); err != nil {
		return nil, err
	}
	PrintHostAddress(db.p2p.Host)

	// init light ipfs node
	ipfs, err := ipfslite.New(ctx, db.store, db.p2p.Host, db.p2p.KadDHT, nil)
	if err != nil {
		return nil, err
	}

	pubsubBC, err := crdt.NewPubSubBroadcaster(ctx, db.p2p.PubSub, db.cfg.DataSyncChannel)
	if err != nil {
		return nil, err
	}

	opts := crdt.DefaultOptions()
	opts.Logger = db.cfg.Logger
	opts.RebroadcastInterval = 5 * time.Second
	opts.PutHook = func(k ds.Key, v []byte) {
		fmt.Printf("Added: [%s] -> %s\n> ", k, string(v))
	}
	opts.DeleteHook = func(k ds.Key) {
		fmt.Printf("Removed: [%s]\n> ", k)
	}

	db.crdt, err = crdt.New(db.store, ds.NewKey(c.Namespace), ipfs, pubsubBC, opts)
	if err != nil {
		return nil, err
	}

	return &db, nil
}

func (c *CRDTKeyValueDB) Close() {
	_ = c.store.Close()
	_ = c.crdt.Close()
	_ = c.p2p.Host.Close()
}

func (c *CRDTKeyValueDB) MarshalPrivateKey() ([]byte, error) {
	return crypto.MarshalPrivateKey(c.privateKey)
}

func (c *CRDTKeyValueDB) nodeNetPubSub(ctx context.Context) error {
	// net pubsub
	netTopic, err := c.p2p.PubSub.Join(c.cfg.NetDiscoveryChannel)
	if err != nil {
		return err
	}

	netSubs, err := netTopic.Subscribe()
	if err != nil {
		return err
	}

	// Use a special pubsub topic to avoid disconnecting
	go func() {
		for {
			msg, err := netSubs.Next(ctx)
			if err != nil {
				fmt.Println(err)
				break
			}
			c.p2p.Host.ConnManager().TagPeer(msg.ReceivedFrom, "keep", 100)
		}
	}()

	go func() {
		tick := time.NewTicker(30 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				_ = netTopic.Publish(ctx, []byte("hi!"))
			}
		}
	}()
	return nil
}

func (c *CRDTKeyValueDB) Put(ctx context.Context, key, value []byte) error {
	return c.crdt.Put(ctx, ds.NewKey(string(key)), value)
}

func (c *CRDTKeyValueDB) Get(ctx context.Context, key []byte) ([]byte, error) {
	return c.crdt.Get(ctx, ds.NewKey(string(key)))
}

func (c *CRDTKeyValueDB) Delete(ctx context.Context, key []byte) error {
	return c.crdt.Delete(ctx, ds.NewKey(string(key)))
}

func (c *CRDTKeyValueDB) Has(ctx context.Context, key []byte) (bool, error) {
	return c.crdt.Has(ctx, ds.NewKey(string(key)))
}

func (c *CRDTKeyValueDB) Batch(ctx context.Context) (datastore.Batch, error) {
	return c.crdt.Batch(ctx)
}

func (c *CRDTKeyValueDB) Query(ctx context.Context) (query.Results, error) {
	q := query.Query{}
	return c.crdt.Query(ctx, q)
}

func (c *CRDTKeyValueDB) Connect(addr string) error {
	bstr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		return err
	}
	inf, err := peer.AddrInfoFromP2pAddr(bstr)
	if err != nil {
		return err
	}
	if err := c.p2p.Host.Connect(context.TODO(), *inf); err != nil {
		return err
	}
	c.p2p.Host.ConnManager().TagPeer(inf.ID, "keep", 100)
	return nil
}

func (c *CRDTKeyValueDB) Repair() error {
	return c.crdt.Repair()
}

func PrintHostAddress(ha host.Host) {
	// Build host multiaddress
	hostAddr, _ := ma.NewMultiaddr(fmt.Sprintf("/ipfs/%s", ha.ID().Pretty()))
	// Now we can build a full multiaddress to reach this host
	// by encapsulating both addresses:
	for _, a := range ha.Addrs() {
		fmt.Println(a.Encapsulate(hostAddr).String())
	}
}

func IsFileExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}
