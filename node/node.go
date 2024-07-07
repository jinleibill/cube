package node

import (
	"cube/utils"
	"cube/worker"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

type Node struct {
	Name            string
	IP              string
	Api             string
	Cores           int64
	Memory          int64
	MemoryAllocated int64
	Disk            int64
	DiskAllocated   int64
	Stats           worker.Stats
	Role            string
	TaskCount       int
}

func NewNode(name string, api string, role string) *Node {
	return &Node{
		Name: name,
		Api:  api,
		Role: role,
	}
}

func (n *Node) GetStats() (*worker.Stats, error) {
	var resp *http.Response
	var err error

	url := fmt.Sprintf("%s/stats", n.Api)
	resp, err = utils.HttpWithRetry(http.Get, url)
	if err != nil {
		msg := fmt.Sprintf("连接失败: %v\n", n.Api)
		log.Println(msg)
		return nil, errors.New(msg)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		msg := fmt.Sprintf("响应错误, stats: %v, %v\n", n.Api, err)
		log.Println(msg)
		return nil, errors.New(msg)
	}

	body, _ := ioutil.ReadAll(resp.Body)
	var stats worker.Stats
	err = json.Unmarshal(body, &stats)
	if err != nil {
		msg := fmt.Sprintf("json 解码错误, 节点: %v\n", n.Name)
		log.Println(msg)
		return nil, errors.New(msg)
	}

	n.Memory = int64(stats.MemTotalKb())
	n.Disk = int64(stats.DiskTotal())

	n.Stats = stats

	return &n.Stats, nil
}
