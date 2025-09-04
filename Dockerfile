FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy config and install dependencies
COPY pyproject.toml poetry.lock* /app/
RUN poetry config virtualenvs.create false \
 && poetry install --only main --no-interaction --no-ansi --no-root

# Copy code after installing deps
COPY . /app/

# Run the script
CMD ["python", "main.py"]
