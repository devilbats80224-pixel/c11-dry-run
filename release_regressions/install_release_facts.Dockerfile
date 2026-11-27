ARG C11_BASE_IMAGE
FROM ${C11_BASE_IMAGE}
COPY --chown=0:0 c11-release /opt/c11-release
