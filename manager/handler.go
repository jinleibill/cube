package manager

import (
	"cube/task"
	"encoding/json"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"log"
	"net/http"
	"time"
)

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	te := task.Event{}
	err := d.Decode(&te)
	if err != nil {
		msg := fmt.Sprintf("json 解码失败: %v\n", err)
		log.Println(msg)
		w.WriteHeader(400)
		e := ErrResponse{
			HTTPStatusCode: 400,
			Message:        msg,
		}
		_ = json.NewEncoder(w).Encode(e)
		return
	}

	a.Manager.AddTask(te)
	log.Printf("添加任务: %v\n", te.Task.ID)
	w.WriteHeader(201)
	_ = json.NewEncoder(w).Encode(te.Task)
}

func (a *Api) GetTaskHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_ = json.NewEncoder(w).Encode(a.Manager.GetTasks())
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		log.Printf("未传递 id 参数\n")
		w.WriteHeader(400)
	}
	tID, _ := uuid.Parse(taskID)
	taskToStop, err := a.Manager.TaskDb.Get(tID.String())
	if err != nil {
		log.Printf("任务 id: %v\n 未找到", tID)
		w.WriteHeader(404)
	}

	te := task.Event{
		ID:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now(),
	}

	taskCopy := taskToStop.(*task.Task)
	te.Task = *taskCopy
	a.Manager.AddTask(te)

	log.Printf("添加任务事件: %v，停止任务: %v\n", te.ID, taskCopy.ID)
	w.WriteHeader(204)
}

func (a *Api) GetNodesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_ = json.NewEncoder(w).Encode(a.Manager.WorkerNodes())
}
