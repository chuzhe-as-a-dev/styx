# Build context is the styx repo root (see docker-compose-user-item.yml), so
# this image can reach both styx-package/ and obol/.
FROM python:3.14.3-slim-trixie

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r styx && useradd -rm -d /usr/local/styx -g styx styx

USER styx

ENV PATH="/usr/local/styx/.local/bin:${PATH}"
ENV PYTHONPATH="/usr/local/styx"

# Install the demo app deps (sanic, watchdog) and the local Styx package.
COPY --chown=styx:styx obol/working-example/requirements.txt /tmp/requirements.txt
COPY --chown=styx:styx styx-package /tmp/styx-package
RUN pip install --upgrade pip && \
    pip install --user -r /tmp/requirements.txt && \
    pip install --user /tmp/styx-package

# Preserve the obol/ layout: app.py loads ../examples/compiled/<example>.py
# relative to its own location, so working-example and examples must keep their
# sibling relationship inside the image.
COPY --chown=styx:styx obol/examples/compiled /usr/local/styx/obol/examples/compiled
COPY --chown=styx:styx obol/working-example /usr/local/styx/obol/working-example

WORKDIR /usr/local/styx/obol/working-example

EXPOSE 8002
