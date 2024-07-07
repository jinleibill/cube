## Cube

使用 Go 语言实现的容器编排器。

## 特性
- 支持多个 worker 节点
- 支持轮询和基于指标统计的调度算法
- 支持基于内存和持久化数据存储
- 接受用户请求，运行或停止任务
- 获取任务和节点列表


## 用法
### 要求
安装 docker 运行时版本 >= 1.43

### 启动
```
// 启动 worker 节点 1
./cube worker --port=5556
// 启动 worker 节点 2
./cube worker --port=5557

// 启动 manager 节点
./cube manager --workers="localhost:5556,localhost:5557"
```

### 下发任务
```
./cube run --filename=add_task.json
```
### 停止任务
```
./cube stop taskID
```
### 查看任务列表
```
./cube status
```
| Task ID                              | NAME             | CREATED       | STATUS    | Container ID                                                     | Image |
|--------------------------------------|------------------|---------------|-----------|------------------------------------------------------------------|-------|
| c05762ce-b55a-45e9-8d2c-d8c3e847b16d | test-container-1 | 3 minutes ago | Completed | 44bb4f9d7e1db4519aff3837278cc30a1718aed485bdc0d8708201848ee1dd66 | nginx |

### 查看节点列表
```
./cube nodes 
```
| NAME           | MEMORY(MB) | DISK(GB) | ROLE   | TASKS |
|----------------|------------|----------|--------|-------|
| localhost:5556 | 1000       | 100      | worker | 0     |
| localhost:5557 | 1200       | 250      | worker | 1     |

## 任务生命周期
| 状态        | 解释              |
|-----------|-----------------|
| Pending   | 用户提交任务，任务入队等待调度 |
| Scheduled | 根据调度算法选择机器并发送任务 |
| Running   | 在所选机器成功运行任务     |
| Completed | 任务完成或者被用户停止     |
| Failed    | 任务执行失败          |

