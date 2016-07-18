FROM jeanblanchard/alpine-glibc
ARG git_commit=unknown
ARG buildenv_git_commit=unknown
ARG version=unknown
COPY job-status-recorder /bin/job-status-recorder
CMD ["job-status-recorder" "--help"]
