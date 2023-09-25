package squirrel

type Cacher interface {
	SetTag(key, tag string, value any) error
	ReadFull(key string, b []byte) (int, error)
	ReadAll(key string, buf []byte) ([]byte, error)
}
