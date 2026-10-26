package jobserver_test

import (
	"context"
	"log"
	"time"

	"github.com/wow-look-at-my/go-jobserver/jobserver"
	"github.com/wow-look-at-my/go-jobserver/priority"
)

type FetchSpec struct {
	jobserver.TaskSpec
	URL string `json:"url"`
}

type PublishSpec struct {
	jobserver.TaskSpec
	Channel string `json:"channel"`
}

// FetchInbox has a Run, so go-jobserver's workers run its tasks.
type FetchInbox struct {
	jobserver.Inbox[*FetchSpec, int64]
	Urgent priority.MultiPriorityKey
}

func (i *FetchInbox) Run(ctx context.Context, s *FetchSpec) (int64, error) { return fetch(ctx, s.URL) }
func (i *FetchInbox) PriorityKeys() []*priority.MultiPriorityKey {
	return []*priority.MultiPriorityKey{&i.Urgent}
}

// PublishInbox has no Run, so a consumer takes its tasks.
type PublishInbox struct {
	jobserver.Inbox[*PublishSpec, string]
}

func (i *PublishInbox) InboxPriority() int { return 1 }

// The order is fetch, then build, then publish. A clean task runs if the build fails.
func ExampleNewInbox() {
	server, err := jobserver.New(jobserver.DefaultConfig(jobserver.DefaultDir()))
	if err != nil {
		log.Fatal(err)
	}
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
	defer server.Close()

	inboxA, err := jobserver.NewInbox[FetchInbox](server, "a")
	if err != nil {
		log.Fatal(err)
	}
	inboxB, err := jobserver.NewInbox[PublishInbox](server, "b")
	if err != nil {
		log.Fatal(err)
	}

	p := inboxA.NewPriority()
	p.Set(&inboxA.Urgent, -1)
	fetchTask, err := inboxA.AddTask(&FetchSpec{
		TaskSpec: jobserver.TaskSpec{Title: "fetch", Timeout: time.Minute},
		URL:      "https://example.com/src.tar",
	}, p)
	if err != nil {
		log.Fatal(err)
	}

	build, err := jobserver.OnSuccess(fetchTask, server.DefaultInbox(), &jobserver.CommandSpec{
		TaskSpec: jobserver.TaskSpec{Title: "build"},
		Command:  []string{"make", "all"},
	})
	if err != nil {
		log.Fatal(err)
	}
	if _, err := jobserver.OnFail(build, server.DefaultInbox(), &jobserver.CommandSpec{
		TaskSpec: jobserver.TaskSpec{Title: "clean"},
		Command:  []string{"make", "clean"},
	}); err != nil {
		log.Fatal(err)
	}
	publish, err := jobserver.OnSuccess(build, &inboxB.Inbox, &PublishSpec{
		TaskSpec: jobserver.TaskSpec{Title: "publish"},
		Channel:  "stable",
	})
	if err != nil {
		log.Fatal(err)
	}

	go consume(inboxB)

	url, taskErr := inboxB.Wait(context.Background(), publish)
	if taskErr != nil {
		log.Fatalf("publish: %v %s", taskErr.State, taskErr.Message)
	}
	log.Print("published at ", url)
}

func consume(inbox *PublishInbox) {
	consumer := inbox.Bind()
	defer inbox.Unbind(consumer)
	for {
		task, spec, ok, err := inbox.Take(context.Background(), consumer, -1)
		if err != nil {
			log.Print(err)
			return
		}
		if !ok {
			continue
		}
		if err := inbox.Done(consumer, task, "https://example.com/"+spec.Channel, nil); err != nil {
			log.Print(err)
		}
	}
}

func fetch(ctx context.Context, url string) (int64, error) { return 0, nil }
