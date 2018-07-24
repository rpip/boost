FROM alpine:latest
MAINTAINER Yao Adzaku <yao.adzaku@gmail.com>

ENV PATH /go/bin:/usr/local/go/bin:$PATH
ENV GOPATH /go

RUN	apk add --no-cache \
	ca-certificates

COPY . /go/src/github.com/rpip/boost

RUN set -x \
	&& apk add --no-cache --virtual .build-deps \
		go \
		git \
		gcc \
		libc-dev \
		libgcc \
	&& cd /go/src/github.com/rpip/boost \
	&& go build -o /usr/bin/boost . \
	&& apk del .build-deps \
	&& rm -rf /go \
	&& echo "Build complete."

ENTRYPOINT ["boost"]

CMD ["--help"]
