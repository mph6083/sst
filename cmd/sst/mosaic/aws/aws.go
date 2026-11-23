package aws

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/sst/sst/v3/cmd/sst/mosaic/aws/appsync"
	"github.com/sst/sst/v3/cmd/sst/mosaic/aws/bridge"
	"github.com/sst/sst/v3/pkg/emulatorbridge"
	"github.com/sst/sst/v3/pkg/flag"
	"github.com/sst/sst/v3/pkg/project"
	"github.com/sst/sst/v3/pkg/project/provider"
	"github.com/sst/sst/v3/pkg/server"
)

var ErrIoTDelay = fmt.Errorf("iot not available")
var ErrAppsyncNotReady = fmt.Errorf("appsync not ready")

func Start(
	ctx context.Context,
	p *project.Project,
	s *server.Server,
	args map[string]interface{},
) error {
	uncasted, _ := p.Provider("aws")
	prov := uncasted.(*provider.AwsProvider)
	config := prov.Config()
	slog.Info("getting endpoint")
	prefix := fmt.Sprintf("/sst/%s/%s", p.App().Name, p.App().Stage)

	var conn bridge.Transport
	if flag.SST_AWS_EMULATOR {
		transport, err := emulatorbridge.Ensure(ctx, config, emulatorbridge.ResolveNames(p.App().Name, p.App().Stage))
		if err != nil {
			return err
		}
		transport.Purge(ctx)
		slog.Info("using aws emulator live bridge")
		conn = transport
	} else {
		appsyncConn, err := dialAppSync(ctx, prov, config)
		if err != nil || appsyncConn == nil {
			return err
		}
		conn = appsyncConn
	}
	client := bridge.NewClient(ctx, conn, "dev", prefix)

	functionsChan := make(chan bridge.Message, 1000)
	tasksChan := make(chan bridge.Message, 1000)

	in := input{
		emulator: flag.SST_AWS_EMULATOR,
		config:   config,
		server:   s,
		client:   client,
		project:  p,
		prefix:   prefix,
	}

	in.msg = functionsChan
	go function(ctx, in)

	in.msg = tasksChan
	go task(ctx, in)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-client.Read():
			if msg.Source == "dev" {
				continue
			}
			functionsChan <- msg
			tasksChan <- msg
		}
	}
}

// dialAppSync waits for the AppSync Events API to answer and connects to it. It returns a nil
// connection without an error when the context ends first.
func dialAppSync(ctx context.Context, prov *provider.AwsProvider, config awssdk.Config) (*appsync.Connection, error) {
	rest, realtime, err := prov.ResolveAppSync(ctx)
	if err != nil {
		return nil, err
	}
	slog.Info("found appsync", "rest", rest, "realtime", realtime)

	now := time.Now()
	for {
		slog.Info("checking if appsync is ready")
		_, err := http.Get("https://" + rest)
		if err != nil {
			slog.Error("appsync not ready", "err", err)
			if time.Since(now) > time.Second*10 {
				return nil, ErrAppsyncNotReady
			}
			select {
			case <-ctx.Done():
				return nil, nil
			case <-time.After(time.Second):
				continue
			}
		}
		break
	}
	return appsync.Dial(ctx, config, rest, realtime)
}
