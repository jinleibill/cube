package worker

import (
	"cube/task"
	"encoding/json"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"log"
	"net/http"
)

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	te := task.Event{}
	err := d.Decode(&te)
	if err != nil {
		msg := fmt.Sprintf("json 解码失败: %v\n", err)
		log.Printf(msg)
		w.WriteHeader(400)
		e := ErrResponse{
			HTTPStatusCode: 400,
			Message:        msg,
		}
		_ = json.NewEncoder(w).Encode(e)
		return
	}

	a.Worker.AddTask(te.Task)
	log.Printf("添加任务: %v\n", te.Task.ID)
	w.WriteHeader(201)
	_ = json.NewEncoder(w).Encode(te.Task)
}

func (a *Api) GetTaskHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_ = json.NewEncoder(w).Encode(a.Worker.GetTasks())
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		log.Printf("未传递 id 参数\n")
		w.WriteHeader(400)
	}

	tID, _ := uuid.Parse(taskID)
	taskToStop, err := a.Worker.Db.Get(tID.String())
	if err != nil {
		log.Printf("任务 id: %v\n 未找到", tID)
		w.WriteHeader(404)
	}
	// 复制原任务作为期望状态任务
	taskCopy := *taskToStop.(*task.Task)
	taskCopy.State = task.Completed
	a.Worker.AddTask(taskCopy)

	log.Printf("添加任务: %v，停止容器: %v\n", taskCopy.ID, taskCopy.ContainerID)
	w.WriteHeader(204)
}

func (a *Api) GetStatsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_ = json.NewEncoder(w).Encode(a.Worker.Stats)
}
