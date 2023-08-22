package env

type (
	Env map[string]string
)

var (
	WD string // Working Directory should be the path to main_test.go, and most of the other paths is related to this directory.
)

func (e Env) CopyFrom(envs ...Env) Env {
	for _, me := range envs {
		for k, v := range me {
			e[k] = v
		}
	}
	return e
}

func (e Env) Set(key, value string) Env {
	e[key] = value
	return e
}
