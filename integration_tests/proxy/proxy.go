package proxy

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type Container struct {
	testcontainers.Container
	URI    string
	DBPort string
}

// StartContainer creates an instance of the toxiproxy container type
func StartContainer(ctx context.Context, network string) (*Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "ghcr.io/shopify/toxiproxy:2.5.0",
		ExposedPorts: []string{"8474/tcp", "8666/tcp"},
		WaitingFor:   wait.ForHTTP("/version").WithPort("8474/tcp"),
		Networks: []string{
			network,
		},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	mappedPort, err := container.MappedPort(ctx, "8474")
	if err != nil {
		return nil, err
	}

	mappedDB, err := container.MappedPort(ctx, "8666")
	if err != nil {
		return nil, err
	}

	hostIP, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}

	uri := fmt.Sprintf("%s:%s", hostIP, mappedPort.Port())

	return &Container{Container: container, URI: uri, DBPort: mappedDB.Port()}, nil
}
