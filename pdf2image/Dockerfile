FROM public.ecr.aws/lambda/python:3.11 AS release

WORKDIR ${LAMBDA_TASK_ROOT}
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN set -euo pipefail; \
    if ! grep -q dynaconf requirements.txt; then \
        echo "Build context is wrong: run docker build from pdf2image/ so we get the app requirements"; \
        exit 1; \
    fi; \
    python -m pip install --no-cache-dir --disable-pip-version-check \
        -r requirements.txt \
        --target "${LAMBDA_TASK_ROOT}"

COPY pdf2image/ ./pdf2image/

CMD ["pdf2image.service.handler"]
