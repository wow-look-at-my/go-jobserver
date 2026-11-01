# go-jobserver: notes for agents

- Task and job mean the same thing. New code, docs and API names say task. Existing `Job` names stay until a later rename.
- go-jobserver owns all task management. A program that uses it only adds tasks and names their dependencies. Interval tasks are `SetSchedule` and `Inbox.Every` in `jobserver/schedule.go`.
- `backlog/` is the durable batch backlog that webhook-runner's `/backlog` routes serve. Its on-disk format is `map[queue][]item` as indented JSON, and a file written by an older release must still load.
- A `*Task` from this process holds its record (`Task.rec`). Read it through `viewRecord`, under the store lock. Never change a record outside `Store.mu`.
- Build and test with bare `go-toolchain` in the repository root. Never run bare `go`.
