# go-jobserver: notes for agents

- Task and job mean the same thing. New code, docs and API names say task. Existing `Job` names stay until a later rename.
- go-jobserver owns all task management. A program that uses it only adds tasks and names their dependencies. Interval tasks are `SetSchedule` and `Inbox.Every` in `jobserver/schedule.go`.
- Build and test with bare `go-toolchain` in the repository root. Never run bare `go`.
