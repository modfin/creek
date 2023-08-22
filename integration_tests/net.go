package integration_tests

import (
	"context"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/sirupsen/logrus"
)

func setupTestNetwork(dockerNetworkName string) error {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return err
	}
	ctx := context.Background()
	cli.NegotiateAPIVersion(ctx)

	networks, err := cli.NetworkList(ctx, types.NetworkListOptions{
		Filters: filters.NewArgs(filters.Arg("name", dockerNetworkName)),
	})
	if err != nil {
		return err
	}

	if len(networks) > 0 {
		// TODO: Check properties of the found networks and return only if name and properties of our desired bridge match those properties, for now, just assume that it's OK if we gat a match...
		logrus.Tracef("Found networks: %v", networks)
		return nil
	}

	// Create test network bridge...
	res, err := cli.NetworkCreate(ctx, dockerNetworkName, types.NetworkCreate{
		CheckDuplicate: true,     // bool
		Driver:         "bridge", // string
		Scope:          "local",  // string
		Attachable:     true,     // bool
	})
	if err != nil {
		return err
	}
	logrus.Infof("Network created: %v", res)
	return nil
}
