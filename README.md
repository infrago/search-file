# search-file

`search-file` 是 `search` 模块的 `file` 驱动。

## 安装

```bash
go get github.com/infrago/search@latest
go get github.com/infrago/search-file@latest
```

## 接入

```go
import (
    _ "github.com/infrago/search"
    _ "github.com/infrago/search-file"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[search]
driver = "file"
```

## 公开 API（摘自源码）

- `func (d *fileDriver) Connect(inst *search.Instance) (search.Connection, error)`
- `func (c *fileConnection) Open() error`
- `func (c *fileConnection) Capabilities() search.Capabilities`
- `func (c *fileConnection) Close() error`
- `func (c *fileConnection) SyncIndex(name string, index search.Index) error`
- `func (c *fileConnection) Clear(name string) error`
- `func (c *fileConnection) Upsert(index string, rows []Map) error`
- `func (c *fileConnection) Delete(index string, ids []string) error`
- `func (c *fileConnection) Search(index string, query search.Query) (search.Result, error)`
- `func (c *fileConnection) Count(index string, query search.Query) (int64, error)`

## 排错

- driver 未生效：确认模块段 `driver` 值与驱动名一致
- 连接失败：检查 endpoint/host/port/鉴权配置
