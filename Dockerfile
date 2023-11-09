FROM python:3.10-slim
ENV TZ="America/Sao_Paulo"

WORKDIR /app
COPY pyproject.toml service.py /app/
COPY credentials /app/credentials
COPY kami_pricing /app/kami_pricing/
RUN pip install poetry && \
    poetry install --no-dev

CMD ["poetry", "run", "python", "service.py"]