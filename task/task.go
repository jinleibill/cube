package task

import (
	"context"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"io"
	"log"
	"os"
	"time"
)

type State int

const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Failed
)

func (s State) String() []string {
	return []string{"Pending", "Scheduled", "Running", "Completed", "Failed"}
}

type Task struct {
	ID           uuid.UUID
	ContainerID  string
	Name         string
	State        State
	Image        string
	Cpu          float64
	Memory       int64
	Disk         int64
	ExposedPorts nat.PortSet
	PortBindings map[string]string
	HostPorts    nat.PortMap
	// 任务异常策略
	RestartPolicy string
	Healthcheck   string
	RestartCount  int
	StartTime     time.Time
	FinishTime    time.Time
}

type Event struct {
	ID        uuid.UUID
	State     State
	Timestamp time.Time
	Task      Task
}

type Config struct {
	Name          string
	AttachStdin   bool
	AttachStdout  bool
	AttachStderr  bool
	ExposedPorts  nat.PortSet
	Cmd           []string
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	Env           []string
	RestartPolicy string
}

func NewConfig(t *Task) *Config {
	return &Config{
		Name:          t.Name,
		Image:         t.Image,
		Cpu:           t.Cpu,
		Memory:        t.Memory,
		Disk:          t.Disk,
		ExposedPorts:  t.ExposedPorts,
		RestartPolicy: t.RestartPolicy,
	}
}

type Docker struct {
	Client *client.Client
	Config Config
}

func NewDocker(c *Config) *Docker {
	dc, _ := client.NewClientWithOpts(client.FromEnv, client.WithVersion("1.43"))
	return &Docker{
		Client: dc,
		Config: *c,
	}
}

type DockerResult struct {
	Error       error
	Action      string
	ContainerId string
	Result      string
}

type DockerInspectResponse struct {
	Error     error
	Container *types.ContainerJSON
}

func (d *Docker) Run() DockerResult {
	ctx := context.Background()
	reader, err := d.Client.ImagePull(
		ctx,
		d.Config.Image,
		image.PullOptions{},
	)
	if err != nil {
		log.Printf("拉取镜像 %s, 错误: %v", d.Config.Image, err)
		return DockerResult{Error: err}
	}
	_, _ = io.Copy(os.Stdout, reader)

	rp := container.RestartPolicy{
		Name: container.RestartPolicyMode(d.Config.RestartPolicy),
	}

	r := container.Resources{
		Memory: d.Config.Memory,
	}

	cc := container.Config{
		Image:        d.Config.Image,
		Tty:          false,
		Env:          d.Config.Env,
		ExposedPorts: d.Config.ExposedPorts,
	}

	hc := container.HostConfig{
		RestartPolicy:   rp,
		Resources:       r,
		PublishAllPorts: true,
	}

	resp, err := d.Client.ContainerCreate(ctx, &cc, &hc, nil, nil, d.Config.Name)
	if err != nil {
		log.Printf("创建容器失败，镜像名 %s, 错误: %v", d.Config.Image, err)
		return DockerResult{Error: err}
	}
	err = d.Client.ContainerStart(ctx, resp.ID, container.StartOptions{})
	if err != nil {
		log.Printf("启动容器失败，容器 id: %s, 错误: %v", resp.ID, err)
		return DockerResult{Error: err}
	}

	out, err := d.Client.ContainerLogs(ctx, resp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		log.Printf("获取日志失败，容器 id：%s, 错误: %v", resp.ID, err)
		return DockerResult{Error: err}
	}

	_, _ = stdcopy.StdCopy(os.Stdout, os.Stderr, out)
	return DockerResult{ContainerId: resp.ID, Action: "start", Result: "success"}
}

func (d *Docker) Stop(id string) DockerResult {
	ctx := context.Background()
	err := d.Client.ContainerStop(ctx, id, container.StopOptions{})
	if err != nil {
		log.Printf("停止容器失败，容器 id：%s，错误 %v", id, err)
		return DockerResult{Error: err}
	}

	err = d.Client.ContainerRemove(ctx, id, container.RemoveOptions{})
	if err != nil {
		log.Printf("移除容器失败，容器 id：%s，错误 %v", id, err)
		return DockerResult{Error: err}
	}

	return DockerResult{Action: "stop", Result: "success"}
}

func (d *Docker) Inspect(id string) DockerInspectResponse {
	ctx := context.Background()
	resp, err := d.Client.ContainerInspect(ctx, id)
	if err != nil {
		log.Printf("检视容器错误: %s\n", err)
		return DockerInspectResponse{Error: err}
	}

	return DockerInspectResponse{Container: &resp}
}
