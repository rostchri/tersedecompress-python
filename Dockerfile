FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install -e ".[test]"
# Test data must be mounted at /test-data
ENV TEST_DATA_DIR=/test-data
CMD ["pytest", "tests/", "-v", "--tb=short"]
