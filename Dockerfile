FROM golang:1.7-alpine

COPY . /go/src/github.com/cyverse-de/job-status-recorder
RUN go install github.com/cyverse-de/job-status-recorder

ENTRYPOINT ["job-status-recorder"]
CMD ["--help"]

ARG git_commit=unknown
ARG version="2.9.0"

LABEL org.cyverse.git-ref="$git_commit"
LABEL org.cyverse.version="$version"
