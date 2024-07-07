package cmd

import (
	"fmt"
	"log"
	"net/http"

	"github.com/spf13/cobra"
)

// stopCmd represents the stop command
var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "停止一个新任务",
	Long:  `停止一个新任务`,
	Run: func(cmd *cobra.Command, args []string) {
		manager, _ := cmd.Flags().GetString("manager")

		url := fmt.Sprintf("http://%s/tasks/%s", manager, args[0])
		client := &http.Client{}
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			log.Fatalf("创建请求失败: %v, 错误: %v\n", url, err)
		}

		resp, err := client.Do(req)
		if err != nil {
			log.Fatalf("连接失败: %v, 错误: %v\n", url, err)
		}

		if resp.StatusCode != http.StatusNoContent {
			log.Printf("请求错误: %v\n", resp.StatusCode)
		}

		defer resp.Body.Close()
		log.Printf("停止任务 %v, 请求发送成功 !\n", args[0])
	},
}

func init() {
	rootCmd.AddCommand(stopCmd)

	stopCmd.Flags().StringP("manager", "m", "localhost:5555", "manager 地址")
}
