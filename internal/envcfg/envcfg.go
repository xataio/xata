package envcfg

import (
	"fmt"
	"maps"
	"os"
	"reflect"
	"sync"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
)

var (
	loadErr  error
	loadOnce sync.Once
)

type BeforeReader interface {
	BeforeRead() error
}

type AfterReader interface {
	AfterRead() error
}

var ifcBefore = ifcConfig{
	t: reflect.TypeFor[BeforeReader](),
	call: func(ifc any) error {
		return ifc.(BeforeReader).BeforeRead() //nolint:forcetypeassert
	},
}

var ifcAfter = ifcConfig{
	t: reflect.TypeFor[AfterReader](),
	call: func(ifc any) error {
		return ifc.(AfterReader).AfterRead() //nolint:forcetypeassert
	},
}

func initEnv() {
	loadOnce.Do(func() {
		files := [2]string{".env", ".env.local"}
		overwrite := map[string]string{}

		// do not use godotenv in order to not error out if
		// .env or .env.local do not exist. Ignore not exists errors
		// and continue with loading/overwrites
		for _, filename := range files {
			f, err := os.Open(filename)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				loadErr = fmt.Errorf("load env: file '%v': %w", filename, err)
				break
			}
			m, err := godotenv.Parse(f)
			f.Close()
			if err != nil {
				loadErr = fmt.Errorf("read env: file '%v': %w", filename, err)
				break
			}

			maps.Copy(overwrite, m)
		}

		if loadErr == nil {
			// Add environment variables that have not been available
			// during process startup.
			for k, v := range overwrite {
				if _, exists := os.LookupEnv(k); !exists {
					os.Setenv(k, v)
				}
			}
		}
	})
}

func Read(cfgs ...any) error {
	initEnv()
	if loadErr != nil {
		return loadErr
	}

	for _, cfg := range cfgs {
		rv := reflect.ValueOf(cfg)

		if err := walkIfcCall(ifcBefore.match, ifcBefore.call, rv); err != nil {
			return err
		}

		if err := cleanenv.ReadEnv(cfg); err != nil {
			return err
		}

		if err := walkIfcCall(ifcAfter.match, ifcAfter.call, rv); err != nil {
			return err
		}
	}

	return nil
}

func Describe(cfg any) (string, error) {
	return cleanenv.GetDescription(cfg, nil)
}

func Getenv(key string) string {
	initEnv()
	return os.Getenv(key)
}
