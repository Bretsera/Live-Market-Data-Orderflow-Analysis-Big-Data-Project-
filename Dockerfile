FROM eclipse-temurin:8-jdk

# Install python3 and venv
RUN apt-get update && apt-get install -y python3 python3-pip python3-venv && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Create venv and install requirements inside it
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code
COPY . .

# Expose dashboard ports
EXPOSE 5000 8050

# Default command
CMD ["python", "data_plotter.py"]

