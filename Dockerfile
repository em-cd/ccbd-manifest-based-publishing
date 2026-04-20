FROM python:3.13-slim

WORKDIR /app

# Copy only requirements first and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the rest of the app
COPY . .

CMD ["python", "-u", "dataset_gen.py"]