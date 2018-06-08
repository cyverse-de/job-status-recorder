FROM golang:1.10.2

RUN go get -u github.com/jstemmer/go-junit-report

COPY . /go/src/github.com/cyverse-de/job-status-recorder
RUN go install github.com/cyverse-de/job-status-recorder


ARG git_commit=unknown
ARG version="2.9.0"
ARG descriptive_version=unknown

LABEL org.cyverse.git-ref="$git_commit"
LABEL org.cyverse.version="$version"
LABEL org.cyverse.descriptive-version="$descriptive_version"
LABEL org.label-schema.vcs-ref="$git_commit"
LABEL org.label-schema.vcs-url="https://github.com/cyverse-de/job-status-recorder"
LABEL org.label-schema.version="$descriptive_version"

ENTRYPOINT ["job-status-recorder"]
