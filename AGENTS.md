# go-jobserver: notes for agents

- Task and job mean the same thing. New code, docs and API names say task. Existing `Job` names stay until a later rename.
- go-jobserver owns all task management. A program that uses it only adds tasks and names their dependencies. Interval tasks are `SetSchedule` and `Inbox.Every` in `jobserver/schedule.go`.
- `backlog/` is the durable batch backlog that webhook-runner's `/backlog` routes serve. Its on-disk format is `map[queue][]item` as indented JSON, and a file written by an older release must still load.
- One record per task: every accessor returns the record, not a copy. Read a field inside `Store.Read`/`Server.Read`, except the fixed fields `Job` lists. Never change a record outside `Store.mu`. Never call into the store, server or scheduler inside `Read`.
- The SQLite database `tasks.sqlite` holds every task. Memory holds open tasks, their dependencies and waited-on tasks, plus a weak map of handed-out records. `journal.log` from an older release moves in once (`store_migrate.go`).
- Build and test with bare `go-toolchain` in the repository root. Never run bare `go`.
