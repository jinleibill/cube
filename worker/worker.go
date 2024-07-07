package worker

import (
	"cube/store"
	"cube/task"
	"errors"
	"fmt"
	"github.com/golang-collections/collections/queue"
	"log"
	"time"
)

type Worker struct {
	Name string
	// 任务临时存放区域
	Queue queue.Queue
	// 存储任务状态
	Db        store.Store
	Stats     *Stats
	TaskCount int
}

func New(name string, taskDbType string) *Worker {
	w := Worker{
		Name:  name,
		Queue: *queue.New(),
	}

	var s store.Store
	switch taskDbType {
	case "memory":
		s = store.NewInMemoryTaskStore()
	case "persistent":
		var err error
		filename := fmt.Sprintf("%s_tasks.db", name)
		s, err = store.NewTaskStore(filename, 0600, "tasks")
		if err != nil {
			log.Fatalf("不能创建任务 store: %v", err)
		}
	}
	w.Db = s
	return &w
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) GetTasks() []*task.Task {
	taskList, err := w.Db.List()
	if err != nil {
		log.Printf("获取任务列表失败: %v\n", err)
		return nil
	}

	return taskList.([]*task.Task)
}

func (w *Worker) CollectionStats() {
	for {
		log.Println("收集 stats")
		w.Stats = GetStats()
		w.Stats.TaskCount = w.TaskCount
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) RunTasks() {
	for {
		if w.Queue.Len() != 0 {
			result := w.runTask()
			if result.Error != nil {
				log.Printf("运行任务错误: %v", result.Error)
			}
		} else {
			log.Printf("任务队列为空")
		}
		log.Println("sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) runTask() task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		log.Println("当前队列没有任务")
		return task.DockerResult{Error: nil}
	}

	taskQueued := t.(task.Task)
	queuedTask, err := w.Db.Get(taskQueued.ID.String())
	if err != nil {
		err = w.Db.Put(taskQueued.ID.String(), &taskQueued)
		if err != nil {
			msg := fmt.Errorf("存储任务 %s, 错误: %v", taskQueued.ID.String(), err)
			log.Println(msg)
			return task.DockerResult{Error: msg}
		}
		queuedTask = &taskQueued
	}
	taskPersisted := *queuedTask.(*task.Task)

	var result task.DockerResult
	if task.ValidateTransitions(taskPersisted.State, taskQueued.State) {
		switch taskQueued.State {
		case task.Scheduled:
			result = w.StartTask(taskQueued)
		case task.Completed:
			result = w.StopTask(taskQueued)
		default:
			result.Error = errors.New("状态转换异常")
		}
	} else {
		err := fmt.Errorf("状态转换无效, 原状态 %v, 目标状态 %v", taskPersisted.State, taskQueued.State)
		return task.DockerResult{Error: err}
	}

	return result
}

func (w *Worker) UpdateTask() {
	for {
		for {
			log.Println("从 docker 检测任务状态")
			w.updateTasks()
			log.Println("任务状态更新完成")
			log.Println("sleeping for 15 seconds")
			time.Sleep(15 * time.Second)
		}
	}
}

func (w *Worker) updateTasks() {
	tasks := w.GetTasks()
	for _, t := range tasks {
		if t.State == task.Running {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				fmt.Printf("错误: %v\n", resp.Error)
			}

			if resp.Container == nil {
				log.Printf("该任务没有运行容器: %s\n", t.ID)
				t.State = task.Failed
				_ = w.Db.Put(t.ID.String(), t)
			}

			if resp.Container.State.Status == "exited" {
				log.Printf("该任务 %s 容器没有运行,状态: %s\n", t.ID, resp.Container.State.Status)
				t.State = task.Failed
				_ = w.Db.Put(t.ID.String(), t)
			}

			t.HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
			_ = w.Db.Put(t.ID.String(), t)
		}
	}
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	result := d.Run()
	if result.Error != nil {
		log.Printf("运行任务失败, 任务: %v, 错误: %v\n", t.ID, result.Error)
		t.State = task.Failed
		_ = w.Db.Put(t.ID.String(), &t)
		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running
	_ = w.Db.Put(t.ID.String(), &t)

	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)

	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		log.Printf("停止容器失败, 容器: %v, 错误: %v\n", t.ContainerID, result.Error)
	}
	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	_ = w.Db.Put(t.ID.String(), &t)
	log.Printf("停止并移除容器: %v, 任务: %v\n", t.ContainerID, t.ID)

	return result
}

func (w *Worker) InspectTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)

	return d.Inspect(t.ContainerID)
}
