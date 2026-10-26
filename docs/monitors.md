# Monitor definitions

`go-jobserver run --monitors <file.xml>` loads the monitor kinds. The document is read with [api-dsl](https://github.com/wow-look-at-my/api-dsl), so `<value>`, `<if>` and `<for>` work in any template content. An element or attribute the loader does not know is a load error.

```xml
<monitors>
	<vars><var name="api">https://api.example.com</var></vars>

	<monitor name="deploy-done" description="the deploy endpoint reports done">
		<arg name="id" required="true" description="the deploy id"/>
		<poll every="30s" timeout="20s">
			<request method="GET" transport="api">
				<url><value name="var.api"/>/deploys/<value name="arg.id"/></url>
				<header name="Accept">application/json</header>
			</request>
		</poll>
		<until>{{ eq .response.state "done" }}</until>
		<describe>deploy is <value name="response.state"/></describe>
	</monitor>

	<monitor name="file-exists">
		<arg name="path" required="true"/>
		<poll every="5s"><argv>test</argv><argv>-e</argv><argv><value name="arg.path"/></argv></poll>
	</monitor>

	<monitor name="log-ready">
		<arg name="path" required="true"/>
		<stream restart="10s"><argv>tail</argv><argv>-F</argv><argv><value name="arg.path"/></argv></stream>
		<until>{{ contains "READY" .line }}</until>
	</monitor>
</monitors>
```

## The check

A monitor has one check.

| Check | Source | Holds when | Cadence |
|---|---|---|---|
| `<poll>` | one or more `<request>` | `<until>` is truthy | `every=` (30s default, 1s floor), `timeout=` per check (20s default) |
| `<poll>` | one or more `<argv>` | `<until>` is truthy; with no `<until>`, exit 0 | the same |
| `<stream>` | one or more `<argv>` | `<until>` is truthy for one stdout line | `restart=` after the process exits |

The loader enforces the separation:

- A `<request>` and `<argv>` never share a check.
- A check holds no text: there is no shell string. A command that needs a shell names `sh` and `-c` as `<argv>` elements, in the open.
- `<stream>` takes only `<argv>`, because a request has no output stream.
- A request or a stream needs `<until>`: nothing else says when the condition holds.
- Several `<request>` elements in one poll each need a unique `name=`. They run in order, and one that fails is the check's error.

## Results

| Outcome | Meaning |
|---|---|
| holds | the monitor completes, and the jobs that depend on it may start |
| not yet | a poll argv exited 1, or `<until>` rendered false |
| check error | a non-2xx status, a transport error, an exit other than 0 or 1, a start failure, a timeout, a template error, or a stream that exited |

A check error is logged at error level and retried on the same cadence. It is never a "not yet". It never ends the monitor.

## Template data

| Field | Value |
|---|---|
| `.arg` | the monitor's args |
| `.var` | the document's vars, overridden by the host's |
| `.status`, `.response`, `.body` | the result of a poll's only request. `.response` is the parsed JSON, or the text |
| `.req.<name>` | the same three fields for each named request |
| `.now` | the time of the check, in Unix seconds |
| `.exit`, `.stdout`, `.stderr` | a poll argv result |
| `.line` | one stream line |

There is no `.env`. Credentials come from a transport: the host registers a named `http.RoundTripper` in `MonitorEnv.Transports`, and `<request transport="name">` uses it.

## Lifecycle

- The job is `running` while it watches, `completed` when the condition holds, `failed` at an explicit deadline, and `cancelled` by interrupt.
- Each check appends one line to the job's log: `<time> check <n>: holds|not yet|ERROR: <detail>`.
- A start puts a watching monitor back to `active`.
