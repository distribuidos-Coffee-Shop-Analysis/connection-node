FROM python:3.9.7-slim

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

# Copy files maintaining directory structure  
COPY main.py /main.py
COPY common/ /common/
COPY server/ /server/
COPY middleware/ /middleware/
COPY protocol/ /protocol/

# Set Python path
ENV PYTHONPATH=/


ENTRYPOINT ["python3", "/main.py"]
