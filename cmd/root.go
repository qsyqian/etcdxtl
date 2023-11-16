package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "etcdxtl",
	Short: "etcdxtl is an extend utils interact with etcd db",
	Long:  `etcdxtl is an extend utils interact with etcd db`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(dumpKVCmd)
}
