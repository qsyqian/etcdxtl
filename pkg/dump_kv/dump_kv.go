package dump_kv

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/openshift/api"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	jsonserializer "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/scheme"
)

type Config struct {
	DBPath    string `json:"DBPath"`
	Key       string `json:"Key"`
	Prefix    bool   `json:"Prefix,omitempty"`
	History   bool   `json:"History,omitempty"`
	OutFormat string `json:"OutFormat"`
}

const KeyBucket = "key"

func init() {
	api.Install(scheme.Scheme)
	api.InstallKube(scheme.Scheme)
}

func DumpKV(dc *Config) error {
	if dc.Prefix {
		return dumpKVWithPrefix(dc)
	}
	return dumpKV(dc)
}

func dumpKVWithPrefix(dc *Config) error {
	db, err := bolt.Open(dc.DBPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		return fmt.Errorf("failed to open bolt DB: %s, %v", dc.DBPath, err)
	}
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(KeyBucket))
		if b == nil {
			return fmt.Errorf("got nil bucket for %s, maybe the db %s is invalid", KeyBucket, dc.DBPath)
		}

		viewedKV := make(map[string]struct{})
		c := b.Cursor()
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			var kv mvccpb.KeyValue
			if err := kv.Unmarshal(v); err != nil {
				klog.Warningf("failed to unmarshal kv, err: %v", err)
				continue
			}

			if strings.HasPrefix(string(kv.Key), dc.Key) {
				_, ok := viewedKV[string(kv.Key)]
				if !ok {
					viewedKV[string(kv.Key)] = struct{}{}
					decodeAndPersistKV(kv, dc.OutFormat)
				}
				if ok && dc.History {
					decodeAndPersistKV(kv, dc.OutFormat)
				}
			}
		}
		return nil
	})
	return err
}

func dumpKV(dc *Config) error {
	db, err := bolt.Open(dc.DBPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		return fmt.Errorf("failed to open bolt DB: %s, %v", dc.DBPath, err)
	}
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(KeyBucket))
		if b == nil {
			return fmt.Errorf("got nil bucket for %s, maybe the db %s is invalid", KeyBucket, dc.DBPath)
		}

		c := b.Cursor()
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			var kv mvccpb.KeyValue
			if err := kv.Unmarshal(v); err != nil {
				klog.Warningf("failed to unmarshal kv, err: %v", err)
				continue
			}
			if string(kv.Key) == dc.Key {
				if err := decodeAndPersistKV(kv, dc.OutFormat); err != nil {
					klog.Errorf("failed to decode and persist kv for %s with revision %d, try next revision, err: %v",
						dc.Key, kv.ModRevision, err)
				}
				if dc.History {
					continue
				}
				return nil
			}
		}
		return nil
	})
	return err
}

func decodeAndPersistKV(kv mvccpb.KeyValue, outFormat string) error {
	decoder := scheme.Codecs.UniversalDeserializer()

	sOptions := jsonserializer.SerializerOptions{
		Strict: true,
	}
	if outFormat == "yaml" {
		sOptions.Yaml = true
	}

	encoder := jsonserializer.NewSerializerWithOptions(jsonserializer.DefaultMetaFactory,
		scheme.Scheme, scheme.Scheme, sOptions)
	objJSONYAML := &bytes.Buffer{}

	obj, _, err := decoder.Decode(kv.Value, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to decode value %q, err: %v", kv.Value, err)
	}

	if err := encoder.Encode(obj, objJSONYAML); err != nil {
		return fmt.Errorf("failed to encoding object %#v, err: %v", obj, err)
	}
	if err = persist(kv.Key, objJSONYAML.Bytes(), kv.ModRevision); err != nil {
		return err
	}
	return nil
}

func persist(key, value []byte, revision int64) error {
	keyStr := string(key[:])
	k := strings.ReplaceAll(strings.TrimPrefix(keyStr, "/"), "/", "-")
	fileName := filepath.Join("/tmp", fmt.Sprintf("%s-%d", k, revision))
	klog.Infof("persist key: %s", fileName)
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(value)
	if err != nil {
		return err
	}
	return nil
}
