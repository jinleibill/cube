package cmd

import (
	"cube/node"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"text/tabwriter"
)

// nodeCmd represents the node command
var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "查看 worker 列表",
	Long:  `允许用户获取 worker 列表`,
	Run: func(cmd *cobra.Command, args []string) {
		manager, _ := cmd.Flags().GetString("manager")

		url := fmt.Sprintf("http://%s/nodes", manager)
		resp, err := http.Get(url)
		if err != nil {
			log.Fatal(err)
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()

		var nodes []*node.Node
		err = json.Unmarshal(body, &nodes)
		if err != nil {
			log.Fatal(err)
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 5, ' ', tabwriter.TabIndent)
		_, _ = fmt.Fprintln(w, "NAME\tMEMORY(MB)\tDISK(GB)\tROLE\tTASKS\t")
		for _, n := range nodes {
			_, _ = fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%d\n", n.Name, n.Memory/1000, n.Disk/1000/1000/1000, n.Role, n.TaskCount)
		}
		_ = w.Flush()
	},
}

func init() {
	rootCmd.AddCommand(nodeCmd)

	nodeCmd.Flags().StringP("manager", "m", "localhost:5555", "manager 地址")
}
