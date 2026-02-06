# EmptyDea-plugin-sdk

这是 EmptyDea 的精简 Go 插件 SDK（给 `DynamicLoader` 用）。

## DynamicLoader 目录约定

运行时目录位于 `EmptyDea_storage/lang/DynamicLoader/`：

- `code/`：插件源码（可选，便于你管理）
- `exe/`：插件编译后的可执行文件（DynamicLoader 从这里启动）
