.PHONY: build install clean run-server run-client test

# 应用名称
APP_NAME := curve

# 构建应用
build:
	@echo "Building $(APP_NAME)..."
	@go build -o $(APP_NAME) .

# 安装到 $GOPATH/bin
install:
	@echo "Installing $(APP_NAME)..."
	@go install .

# 清理构建文件
clean:
	@echo "Cleaning..."
	@rm -f $(APP_NAME) $(APP_NAME).exe curve-*

# 运行服务器（默认端口 8080）
run-server:
	@echo "Starting server..."
	@go run . server --addr localhost:8080

# 运行客户端（连接到默认服务器）
run-client:
	@echo "Starting client..."
	@go run . client --server localhost:8080

# 运行测试
test:
	@echo "Running tests..."
	@go test ./...

# 格式化代码
fmt:
	@echo "Formatting code..."
	@go fmt ./...

# 检查代码
vet:
	@echo "Vetting code..."
	@go vet ./...

# 交叉编译
build-all: build-linux build-darwin build-windows

build-linux:
	@echo "Building for Linux..."
	@GOOS=linux GOARCH=amd64 go build -o $(APP_NAME)-linux-amd64 .

build-darwin:
	@echo "Building for macOS..."
	@GOOS=darwin GOARCH=amd64 go build -o $(APP_NAME)-darwin-amd64 .
	@GOOS=darwin GOARCH=arm64 go build -o $(APP_NAME)-darwin-arm64 .

build-windows:
	@echo "Building for Windows..."
	@GOOS=windows GOARCH=amd64 go build -o $(APP_NAME)-windows-amd64.exe .

# 默认目标
.DEFAULT_GOAL := build
