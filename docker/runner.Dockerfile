FROM python:3.13-slim

WORKDIR /workspace
COPY pyproject.toml requirements.txt README.md ./
COPY src ./src
COPY run_docker_benchmark.py ./
RUN pip install --no-cache-dir -e .

CMD ["python", "run_docker_benchmark.py"]
