package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func Test(cmd *cobra.Command, args []string) {
	fmt.Println("Testing...")
}
