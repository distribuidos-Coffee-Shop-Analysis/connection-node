FROM python:3.9.7-slim

# Copy files maintaining directory structure  
COPY server/main.py /main.py
COPY server/common/ /common/
COPY server/server/ /server/
COPY server/protocol/ /protocol/
COPY server/tests/ /tests/

# Set Python path
ENV PYTHONPATH=/

# Run tests
RUN python -m unittest tests/test_common.py

ENTRYPOINT ["/bin/sh"]