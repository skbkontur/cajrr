VERSION := $(shell git describe --always --tags --abbrev=0 | tail -c +2)
RELEASE := $(shell git describe --always --tags | awk -F- '{ if ($$2) dot="."} END { printf "1%s%s%s%s\n",dot,$$2,dot,$$3}')
VENDOR := "SKB Kontur"
LICENSE := "BSD"
URL := "https://github.com/skbkontur/cajrr"

default: clean prepare test build packages

prepare:
	sudo apt-get -qq update
	sudo apt-get install -y rpm ruby-dev gcc make
	gem install fpm

clean:
	@rm -rf build

prepare: clean

build:
	mvn package

test: clean prepare
	mvn test

up: clean build run

run: build
	/usr/bin/java -jar target/cajrr-1.0-SNAPSHOT.jar server pkg/config.yml
tar:
	mkdir -p build/root/usr/lib/cajrr
	mkdir -p build/root/usr/lib/systemd/system
	mkdir -p build/root/etc/cajrr

	mv target/cajrr-1.0-SNAPSHOT.jar build/root/usr/lib/cajrr/cajrr.jar
	cp pkg/cajrr.service build/root/usr/lib/systemd/system/cajrr.service
	cp pkg/config.yml build/root/etc/cajrr/config.yml

	tar -czvPf build/cajrr-$(VERSION)-$(RELEASE).tar.gz -C build/root  .

rpm:
	fpm -t rpm \
		-s "tar" \
		--description "Cassandra Java Range Repair service" \
		--vendor $(VENDOR) \
		--url $(URL) \
		--license $(LICENSE) \
		--name "cajrr" \
		--version "$(VERSION)" \
		--iteration "$(RELEASE)" \
		--after-install "./pkg/postinst" \
		--config-files "/etc/cajrr/config.yml" \
		-p build \
		build/cajrr-$(VERSION)-$(RELEASE).tar.gz

deb:
	fpm -t deb \
		-s "tar" \
		--description "Cassandra Java Range Repair service" \
		--vendor $(VENDOR) \
		--url $(URL) \
		--license $(LICENSE) \
		--name "cajrr" \
		--version "$(VERSION)" \
		--iteration "$(RELEASE)" \
		--after-install "./pkg/postinst" \
		--config-files "/etc/cajrr/config.yml" \
		-p build \
		build/cajrr-$(VERSION)-$(RELEASE).tar.gz

packages: clean build tar rpm deb

.PHONY: test
