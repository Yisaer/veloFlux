.PHONY: build release test fmt clippy clean help

# 默认目标
.DEFAULT_GOAL := help

SCRIPTS_DIR := $(CURDIR)/scripts
export PATH := $(SCRIPTS_DIR):$(PATH)

# 构建调试版本
build:
	@echo "Building debug version..."
	@cargo build

# 构建发布版本
RELEASE_LEVEL ?= patch

release:
	@echo "Running cargo release ($(RELEASE_LEVEL)) with size-optimized profile..."
	@cargo release $(RELEASE_LEVEL) --workspace --profile release-min --no-tag --no-push --skip-publish --no-confirm --dry-run

# 运行测试
test:
	@echo "Running tests..."
	@cargo test
	@echo "Running tests for sub-crates..."
	@cd src/datatypes && cargo test
	@cd src/flow && cargo test
	@cd src/parser && cargo test

# 格式化代码
fmt:
	@echo "Formatting code..."
	@cargo fmt

# 运行 Clippy 静态检查
clippy:
	@echo "Running Clippy checks..."
	@cargo clippy -- -D warnings
	@echo "Running Clippy checks for sub-crates..."
	@cd src/datatypes && cargo clippy -- -D warnings
	@cd src/flow && cargo clippy -- -D warnings
	@cd src/parser && cargo clippy -- -D warnings

# 清理构建文件
clean:
	@echo "Cleaning build files..."
	@cargo clean

# 显示帮助信息
help:
	@echo "Available targets:"
	@echo "  build   - Build debug version"
	@echo "  release - Build release version"
	@echo "  test    - Run tests"
	@echo "  fmt     - Format code"
	@echo "  clippy  - Run Clippy static analysis"
	@echo "  clean   - Clean build files"
	@echo "  help    - Show this help message"
