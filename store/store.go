package store

import (
	"cube/task"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"os"
)

type Store interface {
	Put(key string, value any) error
	Get(key string) (any, error)
	List() (any, error)
	Count() (int, error)
}

type InMemoryTaskStore struct {
	Db map[string]*task.Task
}

func NewInMemoryTaskStore() *InMemoryTaskStore {
	return &InMemoryTaskStore{
		Db: make(map[string]*task.Task),
	}
}

func (i *InMemoryTaskStore) Put(key string, value any) error {
	t, ok := value.(*task.Task)
	if !ok {
		return fmt.Errorf("值不是任务类型 %v", value)
	}
	i.Db[key] = t
	return nil
}

func (i *InMemoryTaskStore) Get(key string) (any, error) {
	t, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("任务 %v 不存在", key)
	}

	return t, nil
}

func (i *InMemoryTaskStore) List() (any, error) {
	var tasks []*task.Task
	for _, t := range i.Db {
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (i *InMemoryTaskStore) Count() (int, error) {
	return len(i.Db), nil
}

type InMemoryTaskEventStore struct {
	Db map[string]*task.Event
}

func NewInMemoryTaskEventStore() *InMemoryTaskEventStore {
	return &InMemoryTaskEventStore{
		Db: make(map[string]*task.Event),
	}
}

func (i *InMemoryTaskEventStore) Put(key string, value any) error {
	t, ok := value.(*task.Event)
	if !ok {
		return fmt.Errorf("值不是任务事件类型 %v", value)
	}
	i.Db[key] = t
	return nil
}

func (i *InMemoryTaskEventStore) Get(key string) (any, error) {
	t, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("任务事件 %v 不存在", key)
	}

	return t, nil
}

func (i *InMemoryTaskEventStore) List() (any, error) {
	var events []*task.Event
	for _, te := range i.Db {
		events = append(events, te)
	}
	return events, nil
}

func (i *InMemoryTaskEventStore) Count() (int, error) {
	return len(i.Db), nil
}

type TaskStore struct {
	Db        *bolt.DB
	DbFile    string
	FileModel os.FileMode
	Bucket    string
}

func NewTaskStore(file string, model os.FileMode, bucket string) (*TaskStore, error) {
	db, err := bolt.Open(file, model, nil)
	if err != nil {
		return nil, fmt.Errorf("无法打开 %v", file)
	}
	t := TaskStore{
		Db:        db,
		DbFile:    file,
		FileModel: model,
		Bucket:    bucket,
	}
	err = t.CreateBucket()
	if err != nil {
		log.Printf("bucket: %v 已存在", t.Bucket)
	}

	return &t, nil
}

func (ts *TaskStore) CreateBucket() error {
	return ts.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(ts.Bucket))
		if err != nil {
			return fmt.Errorf("创建 bucket %s, 错误: %v", ts.Bucket, err)
		}
		return nil
	})
}

func (ts *TaskStore) Close() error {
	return ts.Db.Close()
}

func (ts *TaskStore) Put(key string, value any) error {
	return ts.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Bucket))

		buf, err := json.Marshal(value.(*task.Task))
		if err != nil {
			return err
		}

		err = b.Put([]byte(key), buf)
		if err != nil {
			return err
		}
		return nil
	})
}

func (ts *TaskStore) Get(key string) (any, error) {
	var t task.Task
	err := ts.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Bucket))
		tt := b.Get([]byte(key))
		if tt == nil {
			return fmt.Errorf("任务 %v 未找到", key)
		}
		err := json.Unmarshal(tt, &t)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (ts *TaskStore) List() (any, error) {
	var tasks []*task.Task
	err := ts.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Bucket))
		_ = b.ForEach(func(k, v []byte) error {
			var t *task.Task
			err := json.Unmarshal(v, &t)
			if err != nil {
				return err
			}
			tasks = append(tasks, t)
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (ts *TaskStore) Count() (int, error) {
	taskCount := 0
	err := ts.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ts.Bucket))
		_ = b.ForEach(func(k, v []byte) error {
			taskCount++
			return nil
		})
		return nil
	})
	if err != nil {
		return -1, err
	}
	return taskCount, nil
}

type TaskEventStore struct {
	Db        *bolt.DB
	DbFile    string
	FileModel os.FileMode
	Bucket    string
}

func NewTaskEventStore(file string, model os.FileMode, bucket string) (*TaskEventStore, error) {
	db, err := bolt.Open(file, model, nil)
	if err != nil {
		return nil, fmt.Errorf("无法打开 %v", file)
	}
	te := TaskEventStore{
		Db:        db,
		DbFile:    file,
		FileModel: model,
		Bucket:    bucket,
	}
	err = te.CreateBucket()
	if err != nil {
		log.Printf("bucket %v 已存在", te.Bucket)
	}

	return &te, nil
}

func (tes *TaskEventStore) CreateBucket() error {
	return tes.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(tes.Bucket))
		if err != nil {
			return fmt.Errorf("创建 bucket %s, 错误: %v", tes.Bucket, err)
		}
		return nil
	})
}

func (tes *TaskEventStore) Close() error {
	return tes.Db.Close()
}

func (tes *TaskEventStore) Put(key string, value any) error {
	return tes.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tes.Bucket))

		buf, err := json.Marshal(value.(*task.Event))
		if err != nil {
			return err
		}

		err = b.Put([]byte(key), buf)
		if err != nil {
			return err
		}
		return nil
	})
}

func (tes *TaskEventStore) Get(key string) (any, error) {
	var te task.Event
	err := tes.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tes.Bucket))
		tet := b.Get([]byte(key))
		if tet == nil {
			return fmt.Errorf("任务事件 %v 未找到", key)
		}
		err := json.Unmarshal(tet, &te)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &te, nil
}

func (tes *TaskEventStore) List() (any, error) {
	var events []*task.Event
	err := tes.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tes.Bucket))
		_ = b.ForEach(func(k, v []byte) error {
			var te *task.Event
			err := json.Unmarshal(v, &te)
			if err != nil {
				return err
			}
			events = append(events, te)
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

func (tes *TaskEventStore) Count() (int, error) {
	taskCount := 0
	err := tes.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tes.Bucket))
		_ = b.ForEach(func(k, v []byte) error {
			taskCount++
			return nil
		})
		return nil
	})
	if err != nil {
		return -1, err
	}
	return taskCount, nil
}
