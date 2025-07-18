FROM python:3.11

# Základní nástroje a SQLite upgrade
RUN apt-get update && apt-get install -y \
    build-essential curl wget gnupg lsb-release \
    libsqlite3-dev gpg software-properties-common

# Odebrání konfliktních ODBC knihoven
RUN apt-get remove -y libodbc2 libodbccr2 libodbcinst2 unixodbc-common || true

# Přidání MS ODBC repozitáře a instalace
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*


# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY . .

# Expose port
EXPOSE 5000

# Set immediate output for python logs (no buffering)
ENV PYTHONUNBUFFERED=1

# Entry point
CMD ["python", "app.py"]
