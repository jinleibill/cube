package cmd

import (
	"cube/task"
	"encoding/json"
	"fmt"
	"github.com/docker/go-units"
	"github.com/spf13/cobra"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"text/tabwriter"
	"time"
)

// statusCmd represents the status command
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "查看任务列表",
	Long:  `允许用户获取任务列表`,
	Run: func(cmd *cobra.Command, args []string) {
		manager, _ := cmd.Flags().GetString("manager")

		url := fmt.Sprintf("http://%s/tasks", manager)
		resp, err := http.Get(url)
		if err != nil {
			log.Fatal(err)
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()

		var tasks []*task.Task
		err = json.Unmarshal(body, &tasks)
		if err != nil {
			log.Fatal(err)
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 5, ' ', tabwriter.TabIndent)
		_, _ = fmt.Fprintln(w, "Task ID\tNAME\tCREATED\tSTATUS\tContainer ID\tImage")
		for _, t := range tasks {
			var start string
			if t.StartTime.IsZero() {
				start = fmt.Sprintf("%s ago", units.HumanDuration(time.Now().UTC().Sub(time.Now().UTC())))
			} else {
				start = fmt.Sprintf("%s ago", units.HumanDuration(time.Now().UTC().Sub(t.StartTime)))
			}

			state := t.State.String()[t.State]
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", t.ID, t.Name, start, state, t.ContainerID, t.Image)
		}
		_ = w.Flush()
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)

	statusCmd.Flags().StringP("manager", "m", "localhost:5555", "manager 地址")
}
