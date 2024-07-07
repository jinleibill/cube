package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "运行一个新任务",
	Long:  `运行一个新任务`,
	Run: func(cmd *cobra.Command, args []string) {
		manager, _ := cmd.Flags().GetString("manager")
		filename, _ := cmd.Flags().GetString("filename")
		fullFilePath, err := filepath.Abs(filename)
		if err != nil {
			log.Fatal(err)
		}
		if !fileExists(fullFilePath) {
			log.Fatalf("文件 %s 不存在", filename)
		}

		log.Printf("manager: %s\n", manager)
		log.Printf("file: %s\n", fullFilePath)

		data, err := os.ReadFile(filename)
		if err != nil {
			log.Fatalf("不能读取文件: %v", filename)
		}
		log.Printf("内容: %s", string(data))

		url := fmt.Sprintf("http://%s/tasks", manager)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Fatal(err)
		}

		if resp.StatusCode != http.StatusCreated {
			log.Printf("请求错误: %v\n", resp.StatusCode)
		}

		defer resp.Body.Close()
		log.Println("创建任务, 请求发送成功 !")
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().StringP("manager", "m", "localhost:5555", "manager 地址")
	runCmd.Flags().StringP("filename", "f", "task.json", "任务文件")
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)

	return !errors.Is(err, os.ErrNotExist)
}
