# go-jobserver

A dependency-ordered job queue that runs as a daemon. Jobs have unique IDs, declare their inputs and outputs, wait for the jobs they depend on, and are skipped when identical work has. Already been done. Every state change is journaled, so a restart resumes exactly where the daemon stopped.

```
go-jobserver queue --output app -- make app
go-jobserver list
```

## Quick start

```sh
go-jobserver run                       # start the daemon
go-jobserver queue -- sh -c 'echo hi'  # hand it a job
go-jobserver list                      # watch it
open http://127.0.0.1:8059/            # the dashboard
```

`go-jobserver run` stays in the foreground and serves every transport. The server directory defaults to `~/.local/state/go-jobserver`, and `-dir` moves it.

## Jobs

A job is a command, the jobs it depends on. The files it reads, and the files it must produce.

| field | meaning |
| --- | --- |
| `id` | the job's unique name, generated as `j-<12 hex>` unless one is given |
| `command` | argv, run directly, or through `sh -c` with `-shell` |
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

A job created with `-draft` waits until `go-jobserver activate <id>`. A dependency that fails makes everything downstream `blocked`, so nothing runs against the wreckage of a broken step.

### Dependency graph

Dependencies are checked when a job is created and whenever one is added. A cycle is refused with `dependency cycle`. The scheduler runs a job once every dependency is `completed` or `cached`.

```sh
go-jobserver queue -draft -name extract -- untar.sh
# prints j-9f2c1a44b7e1
go-jobserver depend j-9f2c1a44b7e1 build
go-jobserver activate j-9f2c1a44b7e1
```

### Outputs and caching

A job that names its outputs has them hashed when it finishes. Those hashes, the job's command, its directory, its environment, its declared inputs, and the identities of its dependencies make up the job's **identity**. When a new job's identity matches one that already succeeded, the new job is marked `cached`. Its work is not repeated, and its artifacts are the earlier job's. Its `cache_of` names the job that did the work.

Change an input file and the identity changes. The job runs again. Pass `-force` to run anyway, or give jobs the same `-key` to have an unfinished one absorbed instead of duplicated.

## Transports

Any of these can enqueue a job. The daemon serves all of them at once.

| transport | how | where |
| --- | --- | --- |
| go-ipc | shared-memory service | name `go-jobserver-<dir hash>` |
| HTTP | JSON API and dashboard | `127.0.0.1:8059` (`-http`) |
| file socket | the same API over a Unix socket | `<dir>/go-jobserver.sock` |
| spool folder | drop a file in a directory | `<dir>/spool` (`-spool`) |
| CLI | `go-jobserver queue ...` | talks to the socket or the service |

The CLI prefers the file socket and falls back to the go-ipc service, so any process that can reach the directory can enqueue.

### Spool files

A file dropped into the spool directory becomes a job and is then moved to `spool/done/`. A file that cannot be read moves to `spool/failed/` with a `.error` sidecar.

- `deploy.json` - a full spec: `{"name":"deploy","command":["make","deploy"],"outputs":["bin/app"]}`
- `list.json` - a bare argv array: `["ls","-l"]`
- `backup.cmd` - one command line: `tar czf /backup.tgz /srv` The `command` field also accepts a single string, which is split on whitespace.

Write spool files to a temporary name and rename them into place: a file is read as soon as it appears.

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

## Pause, resume and interruption

Pause takes effect between jobs: a request to pause stops new jobs from starting, and jobs already running finish. Resume starts scheduling again. Interruption is per job: `interrupt <id>` kills the job's process group, so a job that spawns helpers leaves none of them behind. The job becomes `cancelled` with the output it produced up to that moment.

The paused flag is part of the journal, so a daemon that restarts stays paused.

## CPU oversubscription

Jobs that each want four CPUs on a four-CPU machine do not run faster together than one after the other. They run slower, and everything else on the host does too. The daemon watches CPU use and acts on it in multiple places: before a job starts, and on the processes of a job already running.

### Measuring

Every `-sample` interval (one second by default) the daemon reads the host's process table, once, and turns consecutive reads into rates.

- How many CPUs were busy **across the whole host**, counted from process CPU time. Kernel time spent outside a process is not part of it.
- How many CPUs **each running job's process tree** used. The tree is the job's child plus its descendants plus everything sharing its process group, so a job that spawns helpers is measured as one.
- The daemon accumulates each job's total, and records it on the job as `cpu_seconds` when the job ends.

On Linux the process table comes from `/proc`. Elsewhere it comes from `ps -Ao pid=,ppid=,pgid=,time=,comm=`. On a host with neither, the reading says so instead of reporting a zero.

### Before a job starts

Each job declares what it expects to cost, and the daemon keeps a running total of what the jobs it started have claimed. A job that will push the total over `-budget` (one per CPU by default) stays `active` and starts when enough of the running jobs have finished. A job larger than the whole budget still runs on an otherwise idle daemon, so nothing is starved forever.

The cost a job declares is used first. A job that declares none is priced at what its last run measured, and a job that has never run is priced at `-cost` (one by default).

```sh
go-jobserver queue -cpu 4 -- make -j4
go-jobserver -budget 6 run
```

### While a job runs

When the running jobs together exceed the budget, the daemon applies the controls each job enables to that job's processes. It takes them back when the total is back under budget. Mechanisms, each enabled or disabled per job:

| mechanism | what it does | Linux | macOS | Windows |
| --- | --- | --- | --- | --- |
| `affinity` | pins the process to a CPU set, `-affinity 0,1` | `sched_setaffinity` | the background policy | the host's shell |
| `priority` | raises the process's nice value, `-priority -nice 10` | `setpriority` | `setpriority` | the host's shell |
| `freeze` | suspends the process, `-freeze` | `SIGSTOP` | `SIGSTOP` | asked first |

`affinity` and `priority` are attempted on every host and take effect wherever the call exists. A host that grows the call needs no change here. A call this host's syscall layer does not carry reports `not-applicable` with the reason, which is a missing capability rather than a refusal. A call the host has and declined - lowering a nice value without the privilege to, say - reports `refused`. `freeze` asks the host before it stops anything. A host whose stop signal ends a process reports success for that signal, so the two cannot be told apart from the answer alone. The daemon finds out on a process of its own. The first job that enables `freeze` makes it run one, stop it, resume it, and watch whether it lives through that. When the answer is that the stop signal ends a process, freeze reports `not-applicable` with that reason and the job is left running. Every attempt is recorded with its outcome, and `go-jobserver stats` and the dashboard show the controls in effect.

macOS keeps no per-process CPU mask. Its `affinity` is Apple's background policy. The same state `taskpolicy -b` sets, which keeps the process's work off the performance cores. macOS reports that policy for the process that asks for it. The daemon cannot read back the policy a job's process was under, and taking the mechanism back clears it. Putting a nice value back means lowering it, which POSIX reserves for a privileged process. A daemon running unprivileged reports `refused` when it takes `priority` back.

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

Windows `freeze` is the one that has to wait. The signal this binary can send there ends a process instead of stopping it. The probe above detects exactly that, so freeze reports `not-applicable` with that reason and the job keeps running. A toolchain whose NT syscall layer serves that signal will be found by the same probe, and freeze will start working with no change here.

### Picking processes

Every mechanism can be limited by process. A selector is a comma-separated list of process IDs and process names.

```sh
# freeze everything but this job's ffmpeg helper
go-jobserver queue -freeze -exempt ffmpeg -- transcode.sh

# only touch one process, by name or by pid, and leave its siblings alone
go-jobserver queue -affinity 0 -affinity-only 1234 -- worker
go-jobserver queue -priority -priority-exempt 99,logger -- build
```

`-exempt` and `-only` apply to every mechanism the job enables. `-affinity-exempt`, `-priority-only` and their siblings apply to one mechanism. `-only` restricts a mechanism to the processes it names. `-exempt` takes processes out of it. A process named by `exempt` is never touched, whatever else names it.

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

`go-jobserver policy -freeze -exempt ffmpeg <id>` replaces a job's policy, which is how a mechanism is turned off again. The flags say what the policy is now, so omitting one turns it off. `-no-cpu` turns sampling off entirely.

## Durability

Everything lives under the server directory.

```
journal.log          every state change, append-only
logs/<id>.log        one job's captured output
go-jobserver.sock    the file socket
spool/               jobs dropped in as files
```

A journal record is a length and a CRC32C checksum around a compact binary payload. Consider a record whose length or checksum. That record does not fit is the tail of an append the process died in the middle of: the reader stops there. The reader cuts the file back to the last good record. A job that was `running` when the process died becomes `failed` with `interrupted by daemon restart`, and the output it wrote is kept.

The journal is flushed to disk after every record. Job output is flushed as it is written and synced at every task boundary and once a second while a job runs.

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

`queue` flags: `-name`, `-dep`/`-depends` (repeatable), `-input`, `-output`, `-env`, `-workdir`, `-key`, `-draft`, `-force`, `-wait`, `-shell`. With no command arguments, `queue` reads one command line from standard input.

Global flags: `-dir`, `-http`, `-socket`, `-spool`, `-ipc`, `-j`, `-budget`, `-cost`, `-sample`, `-no-cpu`, `-no-http`, `-no-socket`, `-no-spool`.

## Build and test

```sh
go-toolchain
```

`go-toolchain` builds the org toolchain's way and runs the suite. Plain `go test` cannot resolve the org modules, which carry no versions of their own.

## Package layout

| path | what lives there |
| --- | --- |
| `main.go` | the command line |
| `jobserver/job.go` | the job model, states and the dedup identity |
| `jobserver/journal.go` | the record codec and frame format |
| `jobserver/store.go` | the job table, the journal and the log files |
| `jobserver/scheduler.go` | readiness, blocking, cycles and cache lookup, as pure functions |
| `jobserver/executor.go` | running a command and hashing what it produced |
| `jobserver/process_unix.go` | the process group a job runs in, and killing all of it |
| `jobserver/policy.go` | a job's CPU cost, its control settings and the process selectors |
| `jobserver/measure.go` | the process table, the process tree and CPU rates |
| `jobserver/control.go` | the mechanisms, the host each is available on, and their outcomes |
| `jobserver/affinity_linux.go` | the CPU mask, where the kernel has one |
| `jobserver/govern.go` | the sampling loop and the response to oversubscription |
| `jobserver/server.go` | the daemon: the scheduling loop, the CPU budget and lifecycle control |
| `jobserver/api.go` | the request and response shapes every transport shares |
| `jobserver/ipc.go` | the go-ipc service |
| `jobserver/http.go` | the routes, the dashboard and the file socket |
| `jobserver/spool.go` | the spool directory watcher |
| `jobserver/client.go` | the client the CLI and other programs use |
