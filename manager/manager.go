package manager

import (
	"bytes"
	"cube/node"
	"cube/scheduler"
	"cube/store"
	"cube/task"
	"cube/worker"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"log"
	"net/http"
	"strings"
	"time"
)

type Manager struct {
	Pending queue.Queue
	TaskDb  store.Store
	EventDb store.Store
	Workers []string
	// worker 对应的任务事件
	WorkerTaskMap map[string][]uuid.UUID
	// 任务 对应的 worker
	TaskWorkerMap map[uuid.UUID]string
	LastWorker    int

	workerNodes []*node.Node
	scheduler   scheduler.Scheduler
}

func New(workers []string, schedulerType string, dbType string) *Manager {
	workerTaskMap := make(map[string][]uuid.UUID)
	taskWorkerMap := make(map[uuid.UUID]string)

	var nodes []*node.Node
	for w := range workers {
		workerTaskMap[workers[w]] = []uuid.UUID{}

		nApi := fmt.Sprintf("http://%v", workers[w])
		n := node.NewNode(workers[w], nApi, "worker")
		nodes = append(nodes, n)
	}

	var s scheduler.Scheduler
	switch schedulerType {
	case "round_robin":
		s = &scheduler.RoundRobin{Name: "round_robin"}
	case "e_pvm":
		s = &scheduler.EPvm{Name: "e_pvm"}
	default:
		s = &scheduler.RoundRobin{Name: "round_robin"}
	}

	m := Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
		workerNodes:   nodes,
		scheduler:     s,
	}

	var ts store.Store
	var es store.Store
	switch dbType {
	case "memory":
		ts = store.NewInMemoryTaskStore()
		es = store.NewInMemoryTaskEventStore()
	case "persistent":
		var err error
		ts, err = store.NewTaskStore("tasks.db", 0600, "tasks")
		if err != nil {
			log.Fatalf("不能创建任务 store: %v", err)
		}
		es, err = store.NewTaskEventStore("event.db", 0600, "events")
		if err != nil {
			log.Fatalf("不能创建任务事件 store: %v", err)
		}
	}

	m.TaskDb = ts
	m.EventDb = es

	return &m
}

func (m *Manager) AddTask(te task.Event) {
	m.Pending.Enqueue(te)
}

func (m *Manager) GetTasks() []*task.Task {
	taskList, err := m.TaskDb.List()
	if err != nil {
		log.Printf("获取任务列表失败: %v\n", err)
		return nil
	}

	return taskList.([]*task.Task)
}

func (m *Manager) WorkerNodes() []*node.Node {
	return m.workerNodes
}

func (m *Manager) SelectWorker(t task.Task) (*node.Node, error) {
	candidates := m.scheduler.SelectCandidateNodes(t, m.workerNodes)
	if candidates == nil {
		msg := fmt.Sprintf("没有可用的候选节点用于任务: %v\n", t.ID)
		err := errors.New(msg)
		return nil, err
	}
	scores := m.scheduler.Score(t, candidates)
	log.Printf("节点算分结果: %v\n", scores)
	selectNode := m.scheduler.Pick(scores, candidates)

	return selectNode, nil
}

func (m *Manager) UpdateTasks() {
	for {
		for {
			log.Println("从 workers 检测任务更新状态")
			m.updateTasks()
			log.Println("任务状态更新完成")
			log.Println("sleeping for 15 seconds")
			time.Sleep(15 * time.Second)
		}
	}
}

func (m *Manager) updateTasks() {
	for _, w := range m.Workers {
		log.Printf("检查 worker %v 用于更新任务状态", w)
		url := fmt.Sprintf("http://%s/tasks", w)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("连接失败: %v, 错误: %v\n", w, err)
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("发送请求错误: %v\n", err)
		}

		d := json.NewDecoder(resp.Body)
		var tasks []*task.Task
		if err = d.Decode(&tasks); err != nil {
			fmt.Printf("json 解码失败: %v\n", err.Error())
		}

		m.WorkerTaskMap[w] = []uuid.UUID{}
		for _, t := range tasks {
			log.Printf("更新任务状态: %v\n", t.ID)

			result, err := m.TaskDb.Get(t.ID.String())
			if err != nil {
				log.Printf("[manager] %s\n", err)
				continue
			}
			taskPersisted, ok := result.(*task.Task)
			if !ok {
				log.Printf("%v 不能转换为 task.Task 类型\n", result)
				continue
			}

			m.WorkerTaskMap[w] = append(m.WorkerTaskMap[w], t.ID)
			m.TaskWorkerMap[t.ID] = w

			if taskPersisted.State != t.State {
				taskPersisted.State = t.State
			}

			taskPersisted.StartTime = t.StartTime
			taskPersisted.FinishTime = t.FinishTime
			taskPersisted.ContainerID = t.ContainerID
			taskPersisted.HostPorts = t.HostPorts

			_ = m.TaskDb.Put(taskPersisted.ID.String(), taskPersisted)
		}
	}
}

func (m *Manager) ProcessTasks() {
	for {
		log.Println("读取 Pending 任务事件队列")
		m.SendWork()
		log.Println("sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) SendWork() {
	if m.Pending.Len() > 0 {
		e := m.Pending.Dequeue()
		te := e.(task.Event)
		err := m.EventDb.Put(te.ID.String(), &te)
		if err != nil {
			log.Printf("存储任务事件 %v, 失败: %v\n", te.ID.String(), err)
			return
		}
		log.Printf("从 Pending 任务事件队列, 取出任务事件: %v\n", te)

		// worker 已调度过该任务
		taskWorker, ok := m.TaskWorkerMap[te.Task.ID]
		if ok {
			result, err := m.TaskDb.Get(te.Task.ID.String())
			if err != nil {
				log.Printf("不能调度任务, 失败: %v\n", err)
				return
			}
			persistedTask, ok := result.(*task.Task)
			if !ok {
				log.Println("不能转换为任务类型")
				return
			}
			if te.State == task.Completed && task.ValidateTransitions(persistedTask.State, te.State) {
				m.stopTask(taskWorker, te.Task.ID.String())
				return
			}

			log.Printf("无效请求：存在任务 %s 不能从状态 %v 转换为完成状态\n", persistedTask.ID.String(), persistedTask.State)
			return
		}

		t := te.Task
		w, err := m.SelectWorker(t)
		if err != nil {
			log.Printf("选择 worker 用于任务: %s, 错误: %v\n", t.ID, err)
		}
		log.Printf("选择 worker: %s, 执行任务: %s\n", w.Name, t.ID)
		m.WorkerTaskMap[w.Name] = append(m.WorkerTaskMap[w.Name], t.ID)
		m.TaskWorkerMap[t.ID] = w.Name

		t.State = task.Scheduled
		_ = m.TaskDb.Put(t.ID.String(), &t)

		data, err := json.Marshal(te)
		if err != nil {
			log.Printf("json 编码错误: %v\n", te)
		}

		url := fmt.Sprintf("http://%s/tasks", w.Name)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Printf("连接失败: %v, 错误: %v\n", w, err)
			m.Pending.Enqueue(te)
			return
		}
		d := json.NewDecoder(resp.Body)
		if resp.StatusCode != http.StatusCreated {
			e := worker.ErrResponse{}
			err := d.Decode(&e)
			if err != nil {
				fmt.Printf("json 解码失败: %v\n", err.Error())
				return
			}
			log.Printf("响应错误, code:%v, message:%v\n", e.HTTPStatusCode, e.Message)
			return
		}

		t = task.Task{}
		err = d.Decode(&t)
		if err != nil {
			fmt.Printf("json 解码失败: %v\n", err.Error())
			return
		}
		w.TaskCount++

		log.Printf("%#v\n", t)
	} else {
		log.Println("当前 Pending 任务事件队列，没有任务事件")
	}
}

func (m *Manager) DoHealthChecks() {
	for {
		log.Println("执行任务健康检查")
		m.doHealthChecks()
		log.Println("任务健康检查完成")
		log.Println("sleeping for 10 seconds")
		time.Sleep(60 * time.Second)
	}
}

func (m *Manager) doHealthChecks() {
	tasks := m.GetTasks()
	for _, t := range tasks {
		if t.State == task.Running && t.RestartCount < 3 {
			err := m.checkTaskHealth(*t)
			if err != nil {
				m.restartTask(t)
			}
		} else if t.State == task.Failed && t.RestartCount < 3 {
			m.restartTask(t)
		}
	}
}

func (m *Manager) checkTaskHealth(t task.Task) error {
	log.Printf("调用任务: %s 健康检测: %s\n", t.ID, t.Healthcheck)

	w := m.TaskWorkerMap[t.ID]
	hostPort := func(ports nat.PortMap) *string {
		for k, _ := range ports {
			return &ports[k][0].HostPort
		}
		return nil
	}(t.HostPorts)
	wUrl := strings.Split(w, ":")
	url := fmt.Sprintf("http://%s:%s%s", wUrl[0], *hostPort, t.Healthcheck)
	log.Printf("调用任务: %s 健康检测: %s\n", t.ID, url)
	resp, err := http.Get(url)
	if err != nil {
		msg := fmt.Sprintf("连接健康检查服务失败: %s", url)
		log.Println(msg)
		return errors.New(msg)
	}
	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("健康检查服务错误, 任务: %s", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}
	log.Printf("任务 %s 健康检查服务响应: %v\n", t.ID, resp.Status)

	return nil
}

func (m *Manager) restartTask(t *task.Task) {
	w := m.TaskWorkerMap[t.ID]
	t.State = task.Scheduled
	t.RestartCount++
	_ = m.TaskDb.Put(t.ID.String(), t)

	te := task.Event{
		ID:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      *t,
	}

	data, err := json.Marshal(te)
	if err != nil {
		log.Printf("json 编码错误: %v\n", te)
	}

	url := fmt.Sprintf("http://%s/tasks", w)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("连接失败: %v, 错误: %v\n", w, err)
		m.Pending.Enqueue(te)
		return
	}
	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		e := worker.ErrResponse{}
		err := d.Decode(&e)
		if err != nil {
			fmt.Printf("json 解码失败: %v\n", err.Error())
			return
		}
		log.Printf("响应错误, code:%v, message:%v\n", e.HTTPStatusCode, e.Message)
		return
	}

	newT := task.Task{}
	err = d.Decode(&newT)
	if err != nil {
		fmt.Printf("json 解码失败: %v\n", err.Error())
		return
	}
	log.Printf("%#v\n", newT)
}

func (m *Manager) stopTask(worker string, taskID string) {
	client := &http.Client{}
	url := fmt.Sprintf("http://%s/tasks/%s", worker, taskID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Printf("创建停止任务 %s 请求, 错误： %v\n", taskID, err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("连接失败: %v, 错误: %v\n", url, err)
		return
	}

	if resp.StatusCode != 204 {
		log.Printf("请求响应错误: %v", err)
		return
	}

	log.Printf("停止任务请求已发送 %s\n", taskID)
}
