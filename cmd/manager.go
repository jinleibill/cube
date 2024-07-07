package cmd

import (
	"cube/manager"
	"github.com/spf13/cobra"
	"log"
)

// managerCmd represents the manager command
var managerCmd = &cobra.Command{
	Use:   "manager",
	Short: "用于启动一个 Cube manager 节点",
	Long: `manager 用于对任务编排，并负责:
- 接受用户请求
- 调度任务到 worker 节点
- 当节点发生故障时重新调度任务
- 定期轮询 worker 节点获取任务更新`,
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetInt("port")
		workers, _ := cmd.Flags().GetStringSlice("workers")
		scheduler, _ := cmd.Flags().GetString("scheduler")
		dbType, _ := cmd.Flags().GetString("db-type")

		log.Println("启动 manager")
		m := manager.New(workers, scheduler, dbType)
		api := manager.Api{Address: host, Port: port, Manager: m}

		go m.ProcessTasks()
		go m.UpdateTasks()
		//go m.DoHealthChecks()
		log.Printf("manager API: http://%s:%d\n", host, port)
		api.Start()
	},
}

func init() {
	rootCmd.AddCommand(managerCmd)

	managerCmd.Flags().StringP("host", "H", "0.0.0.0", "主机 IP 地址")
	managerCmd.Flags().IntP("port", "p", 5555, "监听端口")

	managerCmd.Flags().StringSliceP("workers", "w", []string{"localhost:5556"}, "worker 节点列表")
	managerCmd.Flags().StringP("scheduler", "s", "e_pvm", "调度方式: round_robin 或者 e_pvm")
	managerCmd.Flags().StringP("db-type", "d", "memory", "存储数据类型: memory 或者 persistent")
}
