package cmd

import (
	"fmt"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/klog/v2"
	"os"

	"github.com/spf13/cobra"

	"github.com/qsyqian/etcdxtl/pkg/dump_kv"
)

var dumpKVCmd = &cobra.Command{
	Use:   "dump-kv --db-path --key --prefix --history",
	Short: "dump-kv use to dump key and value from etcd db file directly",
	Example: `
dump pod default/nginx-test
etcdxtl dump-kv --db-path=db --key=/registry/pods/default/nginx-test

dump pod default/nginx-test all history key(if has)
etcdxtl dump-kv --db-path=db --key=/registry/pods/default/nginx-test --history=true

dump pods in default
etcdxtl dump-kv --db-path=db --key=/registry/pods/default --prefix=true

dump pods in default all history key(if has)
etcdxtl dump-kv --db-path=db --key=/registry/pods/default --prefix=true --history=true
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := dumpKV(); err != nil {
			klog.Errorf("dump kv return err: %v", err)
			os.Exit(1)
		}
		return nil
	},
}

var dbPath string
var key string
var prefix bool
var history bool
var outFormat string

func init() {
	dumpKVCmd.PersistentFlags().StringVar(&dbPath, "db-path", "", "db path of etcd db")
	dumpKVCmd.PersistentFlags().StringVar(&key, "key", "", "key want to dump")
	dumpKVCmd.PersistentFlags().BoolVar(&prefix, "prefix", false, "prefix with key or not")
	dumpKVCmd.PersistentFlags().BoolVar(&history, "history", false, "whether dump history key and value")
	dumpKVCmd.PersistentFlags().StringVar(&outFormat, "out", "yaml", "out format, yaml or json, default is yaml")
}

func dumpKV() error {
	if err := validateParams(); err != nil {
		return err
	}
	dc := &dump_kv.Config{
		DBPath:    dbPath,
		Key:       key,
		Prefix:    prefix,
		History:   history,
		OutFormat: outFormat,
	}

	dcByte, _ := json.Marshal(&dc)
	klog.Infof("dump kv with flag: %s", string(dcByte[:]))
	if err := dump_kv.DumpKV(dc); err != nil {
		return err
	}
	return nil
}

func validateParams() error {
	if dbPath == "" {
		return fmt.Errorf("db-path must not be empty")
	}
	if key == "" {
		return fmt.Errorf("key must not be empty")
	}
	return nil
}
