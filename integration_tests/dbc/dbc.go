package dbc

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq" // Initialize Postgresql driver
	"github.com/modfin/creek/integration_tests/env"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type Container struct {
	C           testcontainers.Container
	contextPath string
	dockerFile  string
	dbURL       string
	dbConn      *pgxpool.Pool
	Env         env.Env
	fileLogger  *log.Logger

	startConfig struct {
		ctx               *context.Context
		dockerNetworkName string
		dbName            string
	}
}

var (
	DefaultEnv = map[string]string{
		"POSTGRES_PASSWORD": "qwerty",
		"POSTGRES_USER":     "postgres",
	}
	ErrInvalidContextPath = errors.New("invalid context path")
	ErrInvalidDockerFile  = errors.New("invalid Dockerfile")
	ErrNotADirectory      = errors.New("not a directory")
	camel                 = regexp.MustCompile("(^[^A-Z]*|[A-Z]*)([A-Z][^A-Z]+|$)")
)

func New(contextPath, dockerFile string) (*Container, error) {
	fi, err := os.Stat(contextPath)
	if err != nil {
		return nil, fmt.Errorf("context path %s isn't accessible: %w", contextPath, err)
	}
	if !fi.IsDir() {
		return nil, ErrInvalidContextPath
	}

	ap, err := filepath.Abs(contextPath)
	if err != nil {
		return nil, ErrInvalidContextPath
	}

	fi, err = os.Stat(contextPath + dockerFile)
	if err != nil {
		return nil, fmt.Errorf("Dockerfile.postgres %s isn't accessible: %w", dockerFile, err)
	}
	if fi.IsDir() || fi.Size() == 0 {
		return nil, ErrInvalidDockerFile
	}

	_env := env.Env{}
	_env.CopyFrom(DefaultEnv)

	return &Container{
		contextPath: ap,
		dockerFile:  dockerFile,
		Env:         _env,
	}, nil
}

func (c *Container) Accept(l testcontainers.Log) {
	if c.fileLogger != nil {
		c.fileLogger.Printf("%s: %s", l.LogType, l.Content)
	}
}

func (c *Container) Start(ctx context.Context, dockerNetworkName, dbName string) error {
	startTime := time.Now()
	log.Println("Starting up test container...")
	var err error
	var port = "5432/tcp"
	dbURL := func(host string, port nat.Port) string {
		return fmt.Sprintf("postgres://postgres:qwerty@%s:%s/%s", host, port.Port(), dbName)
	}

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    c.contextPath,
			Dockerfile: c.dockerFile,
		},
		Name:         "postgres-test",
		Hostname:     "postgres-test",
		ExposedPorts: []string{port},
		WaitingFor:   wait.ForSQL(nat.Port(port), "postgres", dbURL).WithStartupTimeout(time.Second * 60),
		Networks:     []string{dockerNetworkName},
		Env:          c.Env,
		Cmd:          []string{"postgres", "-c", "wal_level=logical"},
	}
	log.Println("Sending request...")
	c.C, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return err
	}

	c.StartLogger(ctx)

	actualPort, err := c.C.MappedPort(context.Background(), nat.Port(port))
	if err != nil {
		return err
	}

	c.dbURL, c.dbConn, err = c.initDB(dbName, actualPort.Port())
	if err != nil {
		return err
	}

	c.startConfig.ctx = &ctx
	c.startConfig.dockerNetworkName = dockerNetworkName
	c.startConfig.dbName = dbName

	log.Printf("postgres-test test container started (%v)\n", time.Since(startTime))
	return err
}

func (c *Container) StartLogger(ctx context.Context) {
	logFileName := fmt.Sprintf("%s/../logs/container-postgres-test.log", env.WD)
	logFile, err := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("Failed to create log file: %s, no logfile will be created!", logFileName)
	} else {
		c.fileLogger = log.New(logFile, "", log.Ltime|log.Lmicroseconds)
	}
	c.C.FollowOutput(c)
	_ = c.C.StartLogProducer(ctx)
}

func (c *Container) DBURL() string         { return c.dbURL }
func (c *Container) DBConn() *pgxpool.Pool { return c.dbConn }

func (c *Container) initDB(dbName, port string) (string, *pgxpool.Pool, error) {
	log.Printf("Initializing %s DB...", dbName)
	URL := fmt.Sprintf("postgres://postgres:qwerty@localhost:%s/%s", port, dbName)
	DB, err := pgxpool.New(context.Background(), URL)
	if err != nil {
		return URL, nil, err
	}
	err = DB.Ping(context.Background())
	if err != nil {
		return URL, DB, err
	}
	//start := time.Now()
	//err = c.LoadSQL(DB, "../testdb/", "^[012345]\\d{2}-.*\\.sql$", 1, false)
	//if err != nil {
	//	return URL, DB, err
	//}
	//log.Println("Loading test data...")
	//err = c.LoadSQL(DB, "../testdata/", "^.*\\.sql$", 2, false)
	//if err != nil {
	//	return URL, DB, err
	//}
	//log.Println("Adding indexes/views/triggers/etc...")
	//err = c.LoadSQL(DB, "../testdb/", "^[6789]\\d{2}-.*\\.sql$", 1, false)
	//if err != nil {
	//	return URL, DB, err
	//}
	//log.Println("Adding special test data...")
	//err = c.LoadSQL(DB, "../testdata/special/", "^.*\\.sql$", 2, true)
	//log.Printf("Test database loaded in %v", time.Since(start))
	return URL, DB, err
}

func (c *Container) LoadSQL(db *pgxpool.Pool, filePath, fileNameRegexp string, parallel int, recursive bool) error {
	fi, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("can't load SQL files from %s: %w", filePath, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("can't load SQL files from %s: %w", filePath, ErrNotADirectory)
	}
	wc, err := regexp.Compile(fileNameRegexp)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	var fileChan = make(chan string)
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fileName := range fileChan {
				start := time.Now()
				data, err := ioutil.ReadFile(fileName)
				if err != nil {
					log.Printf("Error reading %s, error: %v", fileName, err)
					continue
				}
				_, err = db.Exec(context.Background(), string(data))
				if err != nil {
					log.Printf("Error running %s, error: %v", fileName, err)
					panic(err)
				}
				log.Printf("*  %50s loaded in %v\n", fileName, time.Since(start))
			}
		}()
	}
	err = filepath.Walk(filePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if filePath != path && !recursive {
				return fs.SkipDir
			}
			return nil
		}
		if !wc.MatchString(info.Name()) {
			return nil
		}
		fileChan <- path
		return nil
	})
	if err != nil {
		log.Println("Failed to load SQL data, error:", err)
	}
	close(fileChan)
	wg.Wait()

	return err
}

func (c *Container) Stop(ctx context.Context) error {
	return c.C.Terminate(ctx)
}

func (c *Container) Terminate(ctx context.Context) {
	log.Println("postgres-test: Stopping log producer")
	_ = c.C.StopLogProducer()

	log.Println("postgres-test: Terminating container")
	err := c.C.Terminate(ctx)
	if err != nil {
		log.Printf("Error while terminating postgres-test, error: %v", err)
	}
}

func (c *Container) RestartInCurrentCtx() error {
	if c.startConfig.ctx == nil {
		return errors.New("could not RestartInCurrentCtx, missing context")
	}
	c.Terminate(*c.startConfig.ctx)
	return c.Start(*c.startConfig.ctx, c.startConfig.dockerNetworkName, c.startConfig.dbName)
}

func camelToSnake(s string) string {
	var a []string
	for _, sub := range camel.FindAllStringSubmatch(s, -1) {
		if sub[1] != "" {
			a = append(a, sub[1])
		}
		if sub[2] != "" {
			a = append(a, sub[2])
		}
	}
	return strings.ToLower(strings.Join(a, "_"))
}
