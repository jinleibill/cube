package cmd

import (
	"cube/worker"
	"fmt"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"log"
)

// workerCmd represents the worker command
var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "用于启动一个 Cube worker 节点",
	Long:  `worker 用于运行任务并且响应 manager 的任务请求`,
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetInt("port")
		name, _ := cmd.Flags().GetString("name")
		dbType, _ := cmd.Flags().GetString("db-type")

		log.Println("启动 worker")
		w := worker.New(name, dbType)
		api := worker.Api{Address: host, Port: port, Worker: w}

		go w.RunTasks()
		go w.CollectionStats()
		go w.UpdateTask()
		log.Printf("worker API: http://%s:%d\n", host, port)
		api.Start()
	},
}

func init() {
	rootCmd.AddCommand(workerCmd)

	workerCmd.Flags().StringP("host", "H", "0.0.0.0", "主机 IP 地址")
	workerCmd.Flags().IntP("port", "p", 5556, "监听端口")

	workerCmd.Flags().StringP("name", "n", fmt.Sprintf("worker-%s", uuid.New().String()), "worker 名称")
	workerCmd.Flags().StringP("db-type", "d", "memory", "存储数据类型: memory 或者 persistent")
}
