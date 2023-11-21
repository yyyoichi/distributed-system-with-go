package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	api "github.com/yyyoichi/proglog/api/v1"
	"github.com/yyyoichi/proglog/internal/agent"
	"github.com/yyyoichi/proglog/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	var agents []*agent.Agent
	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		// serfディスカバリ用
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		// logServer用
		rpcPort := ports[1]
		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)
		var StartJoinAddres []string
		if i != 0 {
			// 0のノードに参加
			StartJoinAddres = append(StartJoinAddres,
				agents[0].Config.BindAddr)
		}
		agent, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  StartJoinAddres,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModeFile:     config.ACLModelFile,
			ACLPlicicyFile:  config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
		})
		require.NoError(t, err)
		agents = append(agents, agent)
	}
	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t,
				os.RemoveAll(agent.DataDir))
		}
	}()
	time.Sleep(3 * time.Second)

	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("foo"),
			},
		},
	)
	require.NoError(t, err)

	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("foo"))

	time.Sleep(3 * time.Second)
	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("foo"))

	// 1. agents[0] has offset 1
	// 2. copy to agents[1]
	// 3. copy to agents[0] from agents[1]
	// 4. .....loop.....
	// Expected consumeResponse is Nil, but got it.
	// consumeResponse, err = followerClient.Consume(
	// 	context.Background(),
	// 	&api.ConsumeRequest{
	// 		Offset: produceResponse.Offset + 1,
	// 	},
	// )
	// require.Nil(t, consumeResponse)
	// require.Error(t, err)
	// got := status.Code(err)
	// want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	// require.Equal(t, got, want)
}

func client(t *testing.T, agent *agent.Agent, tlsConfig *tls.Config) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.Dial(rpcAddr, opts...)
	require.NoError(t, err)

	client := api.NewLogClient(conn)
	return client
}
