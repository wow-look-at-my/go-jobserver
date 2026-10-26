# go-jobserver

A dependency-ordered job queue that runs as a daemon. Jobs have unique IDs, declare their inputs and outputs, wait for the jobs they depend on, and are skipped when same work has. Already been done. Every state change is journaled, so a start resumes exactly where the daemon stopped.

```
go-jobserver queue --output app -- make app
go-jobserver list
```

Task and job mean the same thing. New code, docs and API names say task. Existing `Job` names stay until a later rename.

## Quick start

```sh
go-jobserver run                       # start the daemon
go-jobserver queue -- sh -c 'echo hi'  # hand it a job
go-jobserver list                      # watch it
open http://127.0.0.1:8059/            # the dashboard
```

`go-jobserver run` stays in the foreground and serves every transport. The server directory defaults to `~/.local/state/go-jobserver`, and `--dir` moves it.

## Jobs

A job is a command, the jobs it depends on. The files it reads, and the files it must produce.

| field | meaning |
| --- | --- |
| `id` | the job's unique name, generated as `j-<12 hex>` unless one is given |
| `command` | argv, run directly, or through `sh -c` with `--shell` |
| `deps` | job IDs that must succeed first |
| `inputs` | files whose content decides the job's cache identity |
| `outputs` | files the job must produce; missing ones fail the job |
| `dir` | the working directory |
| `env` | extra environment entries |
| `key` | a dedup key shared by jobs that mean the same thing |
| `force` | run even when identical work already finished |

### States

```
draft ──activate──> active ──> running ──> completed
                      │             │
                      │             ├──> failed
                      │             ├──> cancelled   (interrupted)
                      │             └──> cached      (identical work already done)
                      └──> blocked                 (a dependency failed)
```

A job created with `--draft` waits until `go-jobserver activate <id>`. A dependency that fails makes everything downstream `blocked`, so nothing runs against the wreckage of a broken step.

### Dependency graph

Dependencies are checked when a job is created and when one is added. A cycle is refused with `dependency cycle`. The scheduler runs a job once every dependency is `completed` or `cached`.

```sh
go-jobserver queue --draft --name extract -- untar.sh
# prints j-9f2c1a44b7e1
go-jobserver depend j-9f2c1a44b7e1 build
go-jobserver activate j-9f2c1a44b7e1
```

### Outputs and caching

A job that names its outputs has them hashed when it finishes. Those hashes, the job's command, its directory, its environment, its declared inputs, and the identities of its dependencies make up the job's **identity**. When a new job's identity matches one that already succeeded, the new job is marked `cached`. Its work is not repeated, and its artifacts are the earlier job's. Its `cache_of` names the job that did the work.

Change an input file and the identity changes. The job runs again. Pass `--force` to run anyway, or give jobs the same `--key` to have an unfinished one absorbed instead of duplicated.

## Transports

Any of these can enqueue a job. The daemon serves all of them at once.

| transport | how | where |
| --- | --- | --- |
| go-ipc | shared-memory service | name `go-jobserver-<dir hash>` |
| HTTP | JSON API and dashboard | `127.0.0.1:8059` (`--http`) |
| file socket | the same API over a Unix socket | `<dir>/go-jobserver.sock` |
| spool folder | drop a file in a directory | `<dir>/spool` (`--spool`) |
| CLI | `go-jobserver queue ...` | talks to the socket or the service |

The CLI prefers the file socket and falls back to the go-ipc service, so any process that can reach the directory can enqueue.

### Spool files

The daemon opens a `spool.Spool` on its default inbox. A file dropped into the spool directory becomes a task. When the task succeeds, the file moves to `spool/done/`. When the task fails, or the inbox refuses the spec, the file moves to `spool/failed/` with a `.error` sidecar that holds the reason. A file that does not decode is renamed to `<name>.corrupt`, with the reason in `<name>.corrupt.error`. A file that cannot be moved stays in the spool.

The daemon logs that once, and reads the file again only after it changes. At start, the files that wait are added oldest first. A task with no title gets the file name without its extension.

- `deploy.json` - a full spec: `{"title":"deploy","command":["make","deploy"],"outputs":["bin/app"]}`
- `list.json` - a bare argv array: `["ls","-l"]`
- `backup.cmd` - one command line: `tar czf /backup.tgz /srv` The `command` field also accepts a single string, which is split on whitespace.

Write spool files to a temporary name that starts with a dot, and rename them into place. A file is read as soon as it appears, and the spool skips dot files.

The `spool` package works on any inbox. `spool.Open(dir, inbox)` finds the inbox's `AddTask` method through reflection and decodes each file as JSON into its spec type. A spec type with an `UnmarshalText` method also reads a body that is not JSON. `Park(spec)` writes a spec into the spool atomically, and refuses with `spool.ErrFull` at a bound (`WithBounds`, `SetBounds`). A spec that an inbox refuses with `jobserver.ErrStopped` stays in the spool for the next start.

After the spool adds a file, it writes `<name>.task` with the task ID before it waits. A file whose `.task` names a task the inbox's `Lookup` still finds is not added again: the spool waits on that task. This happens when the spool opens again after a crash. A `.task` that names an unknown task, or another version of the file, is removed. The file is added.

### HTTP API

Every route answers JSON, and `POST /api/<op>` takes the same request envelope the other transports use (`{"op":"enqueue","spec":{...}}`).

| method and path | what it does |
| --- | --- |
| `GET /` | the dashboard |
| `GET /job/{id}` | one job with its output |
| `POST /api/jobs` | enqueue; body is a spec, an argv array, or a command line |
| `GET /api/jobs` | every job |
| `GET /api/jobs/{id}` | one job |
| `GET /api/jobs/{id}/logs?offset=&max=` | the job's output as text |
| `POST /api/pause`, `POST /api/resume` | stop and start scheduling |
| `POST /api/interrupt` | cancel every running job |
| `POST /api/interrupt/{id}` | cancel one job |
| `GET /api/stats` | counters for the server |

Operations are `enqueue`, `activate`, `depend`, `deps`, `list`, `get`, `logs`, `pause`, `resume`, `interrupt`, `interrupt-all`, `policy` and `stats`.

A client has several seconds to send its request headers. The client also has a minute to send the whole request, and a couple of minutes of quiet between requests on one connection. A response has no time limit. A slow reader can fetch a large log. The TCP port and the file socket share these limits.

## Pause, resume and interruption

Pause takes effect between jobs: a request to pause stops new jobs from starting, and jobs already running finish. Resume starts scheduling again. Interruption is per job: `interrupt <id>` kills the job's process group, so a job that spawns helpers leaves none of them behind. The job becomes `cancelled` with the output it produced up to that moment.

The paused flag is part of the journal, so a daemon that restarts stays paused.

## CPU oversubscription

Jobs that each want four CPUs on a four-CPU machine do not run faster together than one after the other. They run slower, and everything else on the host does too. The daemon watches CPU use and acts on it in multiple places: before a job starts, and on the processes of a job already running.

### Measuring

Every `--sample` interval (one second by default) the daemon measures:

- How many CPUs the **whole host** used. On Linux this comes from `/proc/stat`. That file counts every process and kernel thread in every container. It is right inside a container too. A host without `/proc/stat` has no such counters. The daemon then cannot read the host's load, which is a configuration error (see below). The number it shows is only an estimate from process CPU time. The estimate misses kernel time and every process outside the daemon's own namespace.
- How many CPUs **each running job** used. For a job that is a process tree, that means the job's child, its descendants and every process in its process group. So a job that starts helpers is measured as one.

The daemon adds up each job's CPU time. It records the total on the job as `cpu_seconds` when the job ends.

The daemon does no sampling of its own. Every reading comes from [simple-stats-api](https://github.com/wow-look-at-my/simple-stats-api)'s `cpusample` package, which it imports. On Linux the process table comes from `/proc`. Elsewhere it comes from `ps -Ao pid=,ppid=,pgid=,time=,comm=`. A host with neither says so in the status instead of reporting zero.

`--stats-url URL` (`StatsURL` in `SchedulerConfig`) reads the host load from a running simple-stats-api instead of this host's `/proc/stat`. The scheduler and a dashboard on that server show the same numbers. A server that cannot be reached, answers late, or lacks `cpu/percent`, `cpu/count` or `system/procs_running` is an unreadable host load (below). Process trees are still read on this host, because a PID only means something in the daemon's own PID namespace.

#### An unreadable host load is a configuration error

The daemon needs the host's own CPU counters. Without them it cannot see work that is not one of its jobs. It cannot see how many tasks want a CPU. It cannot hold a job back for a host that is already full. Under load it keeps starting work. It can destabilize the system.

So the daemon treats this as a configuration error and does not run on quietly:

- It logs an error at the first sample that cannot read the counters, and again every few minutes until it can. The interval is fixed. The line says why the load is unknown. The scheduler writes it to `ErrorLogf`, or to `Logf` when there is no `ErrorLogf`.
- The CPU status carries the reason as `host_error`. The dashboard shows a banner, and `host load unknown` where it shows the host's load.
- One line says when the counters can be read again.

### Before a job starts

Each job declares what it costs: the CPUs it expects to use. The daemon adds up the costs of the jobs it started. A job starts when its cost fits under the budget plus two CPUs. Until then it stays `active`. It starts the moment a running job finishes. `--budget` sets the budget.

The default is one CPU for each CPU of the host. A job that costs more than the whole budget still runs when nothing else runs, so no job waits forever.

The daemon also looks at the host. A job waits when this sum passes the host's CPUs plus two:

- The tasks that want a CPU right now. That includes work that is not a job.
- The cost of the jobs the daemon started that are not measured yet.
- The cost of the job that waits.

Ninja's `-l` does the same with the load average. The load average lags by about a minute, so the daemon counts the tasks that want a CPU right now. This limit does not look at the budget.

A job always starts when none runs. A busy host cannot starve the daemon.

The two extra CPUs are on purpose. Jobs wait for disk and network, and a budget filled exactly leaves CPUs idle. Ninja does the same: its default is more jobs than the host has CPUs. With a small budget the extra is the budget itself, so a budget of one CPU books two. With the budget set to the whole machine, the extra CPUs keep every CPU busy. Start such jobs at a low priority, for example with `nice`, so the rest of the host stays responsive while every CPU is busy.

The cost a job declares is used first. A job that declares none is priced at what its last run measured. Then at what earlier jobs of the same name measured. `--cost` prices a job with neither. The per-name history is kept in `classes.json` in the server directory.

```sh
go-jobserver queue --cpu 4 -- make -j4
go-jobserver --budget 6 run
```

### While a job runs

Every sample, the daemon adds up the CPUs the running jobs use. A job that is not measured yet counts as using its cost. A job that costs more than the whole budget counts as using the whole budget, because the daemon let it run alone.

- At or below the budget plus two CPUs, the daemon leaves the jobs alone.
- Above that, the daemon applies the controls each job enables. It applies them to the jobs that use more than their cost, or to every running job when none does.
- The controls come off when the load stayed at or below the budget for `RevertAfter`. Between the budget and the budget plus two CPUs they stay as they are. A load that moves around the upper line does not switch them on and off.

A control the host refuses is tried again every `RetryInterval`, for as long as the job runs. A negative `RevertAfter` or `RetryInterval` means no wait. Both are `SchedulerConfig` fields. The daemon runs on the defaults:

```go
jobserver.SchedulerConfig{
	RevertAfter:   jobserver.DefaultRevertAfter,   // 5s: how long the load must stay at the budget before the controls come off
	RetryInterval: jobserver.DefaultRetryInterval, // 10s: wait before a refused control is tried again
}
```

No host can use more CPUs than it has. With the budget set to the whole machine, the load never passes the budget plus two CPUs, so the controls never go on. Of the controls, `priority` keeps CPUs busy: it lowers the job's priority and takes no CPU from a host that has none to spare. `freeze` and `affinity` hold a job back. They can leave CPUs idle. Mechanisms, each enabled or disabled per job:

| mechanism | what it does | Linux | macOS | Windows |
| --- | --- | --- | --- | --- |
| `affinity` | pins the process to a CPU set, `--affinity 0,1` | `sched_setaffinity` | the background policy | the host's shell |
| `priority` | raises the process's nice value, `--priority --nice 10` | `setpriority` | `setpriority` | the host's shell |
| `freeze` | suspends the process, `--freeze` | `SIGSTOP` | `SIGSTOP` | asked first |

`affinity` and `priority` are attempted on every host and take effect wherever the call exists. A host that grows the call needs no change here. A call this host's syscall layer does not carry reports `not-applicable` with the reason, which is a missing capability rather than a refusal. A call the host has and declined - lowering a nice value without the privilege to, say - reports `refused`. `freeze` asks the host before it stops anything. A host whose stop signal ends a process reports success for that signal, so the two cannot be told apart from the answer alone.

The daemon finds out on a process of its own. The first job that enables `freeze` makes it run one, stop it, resume it, and watch whether it lives through that. When the answer is that the stop signal ends a process, freeze reports `not-applicable` with that reason and the job is left running. Every try is recorded with its outcome, and `go-jobserver stats` and the dashboard show the controls in effect.

macOS keeps no per-process CPU mask. Its `affinity` is Apple's background policy. The same state `taskpolicy -b` sets, which keeps the process's work off the performance cores. macOS reports that policy for the process that asks for it. The daemon cannot read back the policy a job's process was under, and taking the mechanism back clears it. Putting a nice value back means lowering it, which POSIX reserves for a privileged process.

A daemon running unprivileged reports `refused` when it takes `priority` back.

### What the control layer calls

Every control the daemon has on macOS and Linux goes to a facility the host has.

| what it does | the call | Linux | macOS |
| --- | --- | --- | --- |
| pin a CPU set | `sched_setaffinity`, `sched_getaffinity` | the kernel | not offered; `affinity` is the background policy |
| move the background policy | `setpriority` with `PRIO_DARWIN_PROCESS` | not offered | libc |
| change a nice value | `setpriority`, `getpriority` | the kernel | libc |
| suspend and resume | `kill` with `SIGSTOP` and `SIGCONT` | the kernel | libc |
| end a job's process tree | `kill` with a negative pid and `SIGKILL` | the kernel | libc |
| put a child in its own process group | `setpgid` | the kernel | libc |
| name the host | `uname` | the kernel | libc, read from `sysctl` |
| list the host's processes | `/proc` | the kernel | `ps` |

One cosmo binary carries the Linux CPU-mask calls to every host it runs on. The daemon checks the host before it makes them, so a host without them is never asked. No control reports a call the host will answer with `ENOSYS`.

A Windows host has no syscall this binary can reach for a process's CPU set or its priority class. `affinity` and `priority` there go through the host's own shell instead - the same shape as a unix supervisor that reaches for `renice`. Both read the state the process is in before they change it. A revert puts that back, and a host that refuses the request answers in its own words, which travel with the outcome. A host with no PowerShell at all reports `not-applicable`, which is the same answer any host gives for a facility it does not have.

Windows `freeze` is the one that has to wait. The signal this binary can send there ends a process instead of stopping it. The probe above detects exactly that, so freeze reports `not-applicable` with that reason and the job keeps running. The same probe will find a toolchain whose NT syscall layer serves that signal, and freeze will start working with no change here.

### Picking processes

Process can limit every mechanism. A selector is a comma-separated list of process IDs and process names.

```sh
# freeze everything but this job's ffmpeg helper
go-jobserver queue --freeze --exempt ffmpeg -- transcode.sh

# only touch one process, by name or by pid, and leave its siblings alone
go-jobserver queue --affinity 0 --affinity-only 1234 -- worker
go-jobserver queue --priority --priority-exempt 99,logger -- build
```

`--exempt` and `--only` apply to every mechanism the job enables. `--affinity-exempt`, `--priority-only` and their siblings apply to one mechanism. `--only` restricts a mechanism to the processes it names. `--exempt` takes processes out of it. A process named by `exempt` is never touched, whatever else names it.

Per-mechanism selectors are also part of the job spec, so the JSON transports carry them:

```json
{
  "command": ["transcode.sh"],
  "policy": {
    "cost": 2,
    "freeze": {"enabled": true, "exempt": {"names": ["ffmpeg"], "pids": [1234]}},
    "affinity": {"enabled": true, "cpus": [0, 1], "only": {"names": ["worker"]}}
  }
}
```

`go-jobserver policy --freeze --exempt ffmpeg <id>` replaces a job's policy, which is how a mechanism is turned off again. The flags say what the policy is now, so omitting one turns it off. `--no-cpu` turns sampling off fully.

## Scheduling work another program runs

The daemon's admission and CPU governance live in `jobserver.Scheduler`, and the daemon's own jobs are only one client of it. A program that runs its own work, such as containers, takes a **lease** for each piece of work and hands the scheduler what to measure. [webhook-runner](https://github.com/wow-look-at-my/webhook-runner) admits every hook run this way.

```go
sched := jobserver.NewScheduler(jobserver.SchedulerConfig{
	MaxConcurrent: 64,                // the count cap
	CPUBudget:     8,                 // CPUs the running jobs may use together
	DefaultCost:   0.25,              // a lease with no cost and no history
	HistoryPath:   "classes.json",    // per-class measured cost
	PriorityKeys: []jobserver.PriorityKey{ // admission order, top row first, lower first
		{Name: "urgency", Description: "how soon the caller needs it"},
		{Name: jobserver.KeyOpenBatch},
	},
})
sched.Start()
sched.SetGroups(map[string]int{"deploy": 1})

lease, err := sched.Acquire(ctx, jobserver.LeaseRequest{
	ID: runID, Class: "build", Group: "deploy", Priority: map[string]float64{"urgency": -5}, Batch: "wf-123",
	Policy: jobserver.JobPolicy{Priority: jobserver.PrioritySettings{Enabled: true, Nice: 10}},
	Limits: jobserver.Limits{MaxCPUs: 4, MaxCPUSeconds: 3600},
}, func(q jobserver.QueueState) { /* where the lease waits, and why */ })
defer lease.Release()
lease.AttachProcess(pid)       // or lease.AttachActuator(a), for work that is not a process tree here
select {
case <-lease.Revoked():        // the scheduler ended the work: lease.RevokeReason() says why
case <-done:
}
```

**Admission.** A lease is granted when its group has a free slot and, unless it is `GroupOnly`, when the count cap has room. Its cost fits the CPU budget plus two CPUs (see "Before a job starts"). Priority keys sort queued leases. `SchedulerConfig.PriorityKeys` lists them, each with a hyphenated name, a description and a default. A lease sets values by name in `LeaseRequest.Priority`.

A key it leaves out takes the key's default. The scheduler compares the keys from the top of the list down, lower first. The first key that differs decides, and arrival order breaks a full tie. A name the list does not hold, and a value or a default that is not a whole number, are refused.

`open-batch` (`KeyOpenBatch`) is the scheduler's own key: 0 while a member of the lease's batch holds a slot or did within `BatchGrace`, 1 otherwise. A list that leaves it out gets it last. `Status` reports the key list, and each lease's value for every key.

A lease that starts a new batch pulls the rest of that batch in right behind it. A lease's cost is what it declares, then its class's measured history, then the default.

Every queued lease is told what it waits on (`cap`, `group` or `cpu`), its position and a reason.

**Governance.** Every sample, the governor measures each running job through its process tree or its `Actuator`. It then acts on what it measured:

| condition | action |
| --- | --- |
| the running jobs use more than the budget plus two CPUs | the policy's mechanisms on the leases that use more than their cost, or on all of them when none does |
| a lease uses more than `Limits.MaxCPUs` | `Actuator.Cap` at the limit, and revoked after `Limits.Grace` when it cannot be capped |
| a lease passes `Limits.MaxCPUSeconds` | revoked |

When a lease is released, the scheduler reverts its controls and records what it used in the history of its class. Its CPU time over the time it was measured. The next lease of that class costs that much.

An `Actuator` measures and controls work the governor cannot see as a process tree: `Usage` reports CPUs used. `Apply`, `Revert` and `Cap` carry out a mechanism. `Status`, `Readings`, `Controls`, `Events` and `Classes` report the live state.

## Inboxes

An inbox is how a task is submitted. The `default` inbox runs commands through the `Runner`. A named inbox holds tasks of its own spec type. Its `Run` method runs them on the server's workers, or a bound consumer takes them with `Take` and settles them with `Done`.

```go
type FetchInbox struct {
	jobserver.Inbox[*FetchSpec, int64]
	Urgent priority.MultiPriorityKey `priority:"urgent,default=0"`
}

inbox, err := jobserver.NewInbox[FetchInbox](server, "fetch")
p := inbox.NewPriority()
p.Set(&inbox.Urgent, -1)
fetch, err := inbox.AddTask(&FetchSpec{TaskSpec: jobserver.TaskSpec{Title: "fetch"}, URL: url}, p)
build, err := jobserver.OnSuccess(fetch, server.DefaultInbox(), &jobserver.CommandSpec{Command: []string{"make"}})
```

- **Priority.** The `priority.MultiPriorityKey` fields of the inbox type are its keys, in field order. Lower goes first, the first value that differs decides, and arrival breaks a tie. `InboxPriority` orders inboxes against each other. The default inbox's key is `DefaultPriorityKey`, and its tasks go through the same admission.
- **Triggers.** `OnSuccess`, `OnFail` and `OnEnded` add a task that waits on another, in any inbox. A trigger that can no longer fire cancels its task.
- **Consumers.** `Bind` attaches one consumer. A new `Bind` or an `Unbind` fails its checked-out tasks with "the consumer went away". A stale handle gets `ErrNotConsumer`. `Take` returns the queued task that sorts first by `Compare`, then by arrival, and waits for the admission of that task. An admitted task that sorts after it gives back its slot until it is taken. The task holds its slot until `Done`. `Lookup(id)` returns a task of the inbox, or `ErrNotFound`.
- **Start.** The journal holds each task's spec and result as JSON. `NewInbox` with the same name takes them back. A task a consumer held fails, and a task of an inbox with `Run` runs again.
- **Task details.** A consumer reports on a task it holds through the inbox. `SetTitle(c, t, title)`, `Step(c, t, parent, title)`, `EndStep(c, t, step, status, msg)` and `RenameStep(c, t, step, title)` for named steps that nest, `OpenWait(c, t, reason)` and `CloseWait(c, t, wait)` for spans where the task waits, and `Log(c, t, line)` for its log. A `Run` method gets its task from `TaskFromContext(ctx)` and passes the empty `Consumer`. A step still open when the task ends becomes `abandoned`. Past its bound, a task drops its oldest closed step or wait, and `StepsDropped` and `WaitsDropped` count the drops. An open one never goes. A task whose held steps or waits are all open gets `ErrAllOpen`.
- **Reading tasks.** `Details(t)` reads one task: title, state, timings, steps, waits, log size, and `Queue`, what a queued task waits on in the scheduler. `Tasks(since)` lists the tasks of the inbox that changed after a `Seq`. A new `Epoch` or `Pruned` in the page means the reader reads again from zero. `ReadLog(t, offset, max)` reads the log. `Counts()` gives the queued, running and ended tasks. Take and `Counts` read only the tasks of their own inbox.
- **Retention.** The server drops an ended task, its result and its log after `Config.Retention` (`DefaultRetention`), and beyond `Config.RetainCount` ended tasks per inbox (`DefaultRetainCount`), oldest first. It checks every `Config.PruneInterval`. It keeps a task that a task which has not ended still waits on, and a task a `Wait` blocks on. The dedup cache does not follow the tasks: a prune keeps the identity and the artifacts of each piece of work that succeeded, so an identical command reuses them after its source task is gone. `Config.CacheRetention` and `Config.CacheCount` bound the cache on its own, and by default it has no bound. A prune compacts the journal: it writes a new file, syncs it, and renames it over the old one. A queued task stays across a start. The next consumer takes it.
- **Over the wire.** The ops `inbox.add`, `inbox.bind`, `inbox.unbind`, `inbox.take`, `inbox.started`, `inbox.done`, `inbox.drain`, `inbox.detail`, `inbox.task`, `inbox.tasks`, `inbox.counts` and `inbox.wait` name their inbox. `ClientInbox` gives the same `*Inbox` methods over a `Client`.

`Spec`, `Server.Enqueue` and the `enqueue` op are deprecated. Each adds the task to the default inbox.

## Durability

Everything lives under the server directory.

```
journal.log          every state change, append-only
classes.json         measured CPU cost per job name
logs/<id>.log        one job's captured output
go-jobserver.sock    the file socket
spool/               jobs dropped in as files
```

A journal record is a length and a CRC32C checksum around a compact binary payload. Consider a record whose length or checksum. That record does not fit is the tail of an append the process died in the middle of: the reader stops there. The reader cuts the file back to the last good record. A job that was `running` when the process died becomes `failed` with `interrupted by daemon restart`, and the output it wrote is kept.

The journal is flushed to disk after every record, except the details a task reports, which the next record flushes. Job output is flushed as it is written and synced at every task boundary and once a second while a job runs.

## Command line

```
go-jobserver [global flags] <command> [args]

run                          run the daemon in the foreground
queue [flags] cmd...         enqueue a job and print its id
list                         list jobs
status <id>                  show one job
logs [-f] <id>               print a job's output
activate <id>                move a draft job to active
depend <id> <dep>...         add dependencies
pause | resume               stop and start scheduling
interrupt <id|all>           stop running jobs
policy [flags] <id>          replace a job's CPU cost and process controls
stats                        show server counters, CPU use and active controls
version                      print the version
```

`queue` flags: `--name`, `--dep`/`--depends` (repeatable), `--input`, `--output`, `--env`, `--workdir`, `--key`, `--draft`, `--force`, `--wait`, `--shell`. With no command arguments, `queue` reads one command line from standard input.

Global flags: `--dir`, `--http`, `--socket`, `--spool`, `--ipc`, `-j`/`--jobs`, `--budget`, `--cost`, `--sample`, `--no-cpu`, `--stats-url`, `--no-http`, `--no-socket`, `--no-spool`. A global flag goes before or after the command. `go-jobserver <command> --help` prints the full flag list for that command.

## Build and test

```sh
go-toolchain
```

`go-toolchain` builds the org toolchain's way and runs the suite. Plain `go test` cannot resolve the org modules, which carry no versions of their own.

## Package layout

| path | what lives there |
| --- | --- |
| `main.go`, `root.go`, `cmd_*.go` | the command line: cobra, one file per command |
| `jobserver/job.go` | the job model, states and the dedup identity |
| `jobserver/journal.go` | the record codec and frame format |
| `jobserver/store.go` | the job table, the journal and the log files |
| `jobserver/scheduler.go` | readiness, blocking, cycles and cache lookup, as pure functions |
| `jobserver/executor.go` | running a command and hashing what it produced |
| `jobserver/process_unix.go` | the process group a job runs in, and killing all of it |
| `jobserver/policy.go` | a job's CPU cost, its control settings and the process selectors |
| `jobserver/stats.go` | the sampler: simple-stats-api's `cpusample`, local or over HTTP |
| `jobserver/control.go` | the mechanisms, the host each is available on, and their outcomes |
| `jobserver/affinity_linux.go` | the CPU mask, where the kernel has one |
| `jobserver/sched.go` | the Scheduler: the count cap, groups, CPU budget, Acquire and TryAcquire |
| `jobserver/sched_plan.go` | admission order: the key tuple, open batches, arrival, and what each lease waits on |
| `jobserver/sched_class.go` | the per-class cost history |
| `jobserver/sched_status.go` | the Scheduler's status snapshot |
| `jobserver/lease.go` | a lease, its limits and the Actuator interface |
| `jobserver/govern.go` | the sampling loop and the measurement of each running job |
| `jobserver/govern_plan.go` | the limits, the budget decision, and the plans to throttle and revert |
| `jobserver/govern_hostload.go` | the error for a host whose load is unknown |
| `jobserver/govern_ctl.go` | the control calls, made outside the governor's lock, and their retries |
| `jobserver/server.go` | the daemon: the job loop and lifecycle control, admitted through the Scheduler |
| `priority/` | the priority keys of an inbox and the values a task sets for them |
| `jobserver/task.go` | TaskSpec, Task, TaskState, TaskError, CommandSpec and CommandResult |
| `jobserver/inbox.go` | the typed Inbox, NewInbox and the triggers |
| `jobserver/inbox_core.go` | a named inbox's leases, consumer and checked-out tasks |
| `jobserver/inbox_server.go` | opening inboxes, the default inbox, Stop and the local backends |
| `jobserver/inbox_api.go` | the inbox ops and ClientInbox |
| `jobserver/api.go` | the request and response shapes every transport shares |
| `jobserver/ipc.go` | the go-ipc service |
| `jobserver/http.go` | the routes, the dashboard and the file socket |
| `spool/` | the spool: a watched directory of spec files that become tasks of one inbox, and Park, which writes one |
| `daemon.go` | the daemon: the server and the spool on its default inbox |
| `jobserver/graph.go` | each job's dependency depth and its children, for the dashboard and `list` |
| `jobserver/client.go` | the client the CLI and other programs use |
