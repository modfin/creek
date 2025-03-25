package integration_tests

import (
	"context"
	"fmt"
	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modfin/creek/integration_tests/dbc"
	"github.com/modfin/creek/integration_tests/proxy"
	"github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"regexp"
	"sync"
	"time"
)

var (
	postgres      *dbc.Container
	natsContainer testcontainers.Container
	testsStarted  time.Time
	toxiProxy     *proxy.Container
	dbProxi       *toxiproxy.Proxy
)

const DBname = "test_db"

func startTestContainers(ctx context.Context, dockerNetworkName string) error {
	var err error

	// Start toxiproxy first
	toxiProxy, err = proxy.StartContainer(ctx, dockerNetworkName)
	if err != nil {
		logrus.Errorf("Failed to create toxiproxy container: %v", err)
		return err
	}

	postgres, err = dbc.New("./containers/", "Dockerfile.postgres")
	if err != nil {
		logrus.Errorf("Failed to create DB container spec: %v", err)
		return err
	}

	var startError bool
	var wg sync.WaitGroup

	wg.Add(2)
	go startPostgres(ctx, dockerNetworkName, &wg, &startError)
	go startNats(ctx, dockerNetworkName, &wg, &startError)
	wg.Wait()

	ip, err := postgres.C.ContainerIP(ctx)
	if err != nil {
		return err
	}

	toxiClient := toxiproxy.NewClient(toxiProxy.URI)
	dbProxi, err = toxiClient.CreateProxy("postgres", "0.0.0.0:8666", fmt.Sprintf("%s:5432", ip))
	if err != nil {
		logrus.Errorf("Failed to create populate proxies: %v", err)
		return err
	}
	err = dbProxi.Enable()
	if err != nil {
		return err
	}

	if startError {
		log.Fatal("There were errors while starting containers, check log output for more information. Test aborted!")
	}

	log.Print("**********************************************************************")
	log.Printf("*                                                                    *")
	log.Printf("*   DB URL: %-50s    *", postgres.DBURL())
	log.Printf("*                                                                    *")
	log.Print("**********************************************************************")

	testsStarted = time.Now()
	return nil
}

func startNats(ctx context.Context, dockerNetworkName string, wg *sync.WaitGroup, b *bool) {
	defer wg.Done()

	startTime := time.Now()
	log.Println("Starting up integration_tests container...")
	var err error
	var ports = []string{"4222", "8222"}

	strategy := wait.NewHTTPStrategy("/healthz").WithStartupTimeout(time.Second * 2).WithPort("8222").WithMethod("GET")

	req := testcontainers.ContainerRequest{
		Image:        "nats",
		Name:         "nats-integration_tests",
		Hostname:     "nats-integration_tests",
		ExposedPorts: ports,
		WaitingFor:   strategy,
		Networks:     []string{dockerNetworkName},
		Cmd:          []string{"-p", "4222", "--http_port", "8222", "-js"},
	}
	log.Println("Sending request...")
	natsContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic(err)
	}

	log.Printf("nats-integration_tests integration_tests container started (%v)\n", time.Since(startTime))
}

func GetNATSURL() string {
	p, _ := natsContainer.MappedPort(TimeoutContext(time.Second*1), nat.Port("4222"))
	return "nats://127.0.0.1:" + p.Port()
}

func GetDBURL() string {
	dbUri := postgres.DBURL()
	re := regexp.MustCompile(`\d+`)
	return re.ReplaceAllString(dbUri, toxiProxy.DBPort)
}

func EnableProxi() error {
	return dbProxi.Enable()
}

func DisableProxi() error {
	return dbProxi.Disable()
}

func GetDBConn() *pgxpool.Pool {
	return postgres.DBConn()
}

func GetPgContainer() *dbc.Container {
	return postgres
}

func ResetContainer() error {
	return postgres.RestartInCurrentCtx()
}

func LoadSql(files ...string) {
	for _, f := range files {
		err := postgres.LoadSQL(postgres.DBConn(), "testdata", f, 1, false)
		if err != nil {
			panic(err.Error())
		}
	}
}

func startPostgres(ctx context.Context, dockerNetworkName string, wg *sync.WaitGroup, startError *bool) {
	defer wg.Done()
	err := postgres.Start(ctx, dockerNetworkName, DBname)
	if err != nil {
		logrus.Errorf("Failed to start postgres container: %v", err)
		*startError = true
		return
	}
}

func shutdownTestContainers(ctx context.Context) {
	// Insert small delay here so that we have time to flush container logs before we shut down the containers
	time.Sleep(time.Second * 1)

	// time.Sleep(time.Hour) // Enable to keep containers alive while debugging

	log.Print("Shutting down containers..")
	var wg sync.WaitGroup
	terminate := func(container interface {
		Terminate(ctx context.Context, options ...testcontainers.TerminateOption) error
	}) {
		defer wg.Done()
		err := container.Terminate(ctx)
		if err != nil {
			log.Printf("failed to terminate container: %v", err)
		}
	}
	wg.Add(3)
	go terminate(postgres)
	go terminate(natsContainer)
	go terminate(toxiProxy)
	wg.Wait()
}
