NAME := psql-streamer
MAINTAINER:= Igor Novgorodov <igor@novg.net>
DESCRIPTION := Service for streaming PostgreSQL events
LICENSE := MPLv2

GO ?= go
VERSION := $(shell cat VERSION)
OUT := .out
PACKAGE := github.com/blind-oracle/$(NAME)

all: build

build:
	rm -rf $(OUT)
	mkdir -p $(OUT)/root/etc/$(NAME)
	cp $(NAME).toml $(OUT)/root/etc/$(NAME)
	go test ./...
	GOARCH=amd64 GOOS=linux $(GO) build -o $(OUT) -ldflags "-s -w -X main.version=$(VERSION)"

deb:
	make build
	make build-deb ARCH=amd64

rpm:
	make build
	make build-rpm ARCH=amd64

build-deb:
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) \
		--deb-priority optional \
		--category admin \
		--force \
		--url https://$(PACKAGE) \
		--description "$(DESCRIPTION)" \
		-m "$(MAINTAINER)" \
		--license "$(LICENSE)" \
		-a $(ARCH) \
		$(OUT)/$(NAME)=/usr/bin/$(NAME) \
		deploy/$(NAME).service=/usr/lib/systemd/system/$(NAME).service \
		$(OUT)/root/=/

build-rpm:
	fpm -s dir -t rpm -n $(NAME) -v $(VERSION) \
		--force \
		--rpm-compression bzip2 \
		--rpm-os linux \
		--url https://$(PACKAGE) \
		--description "$(DESCRIPTION)" \
		-m "$(MAINTAINER)" \
		--license "$(LICENSE)" \
		-a $(ARCH) \
		--config-files /etc/$(NAME)/$(NAME).toml \
		$(OUT)/$(NAME)=/usr/bin/$(NAME) \
		deploy/$(NAME).service=/usr/lib/systemd/system/$(NAME).service \
		$(OUT)/root/=/
