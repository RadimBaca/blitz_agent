# Database Terapeut

AI Agent Based on Brent Ozar's First Responder Scripts.

This is an AI agent that connects to your SQL Server instance and helps with database administration tasks.
It is a web application that runs in a Docker container and uses OpenAI LLM API.

# Preparation

## Brent Ozar First Responder Kit

This application connects to a SQL Server instance and runs [Brent Ozar's First Responder scripts](https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit).
Therefore, you need to have the scripts installed on your SQL Server instance.
To install the First Responder Kit, you clone the repository and run the `Install-All-Scripts.sql` script against your SQL Server.

```shell
git clone https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit
```

## SQL Server Database Permissions

You need a `username` to your SQL Server database with the following permissions:
```sql
USE dbname;
GO

GRANT VIEW DATABASE STATE TO username;
GRANT VIEW DEFINITION TO username;
GRANT EXECUTE ON [dbo].[sp_Blitz] TO username;
GRANT EXECUTE ON [dbo].[sp_BlitzFirst] TO username;
GRANT EXECUTE ON [dbo].[sp_BlitzIndex] TO username;
GRANT EXECUTE ON [dbo].[sp_BlitzCache] TO username;
GRANT EXECUTE ON [dbo].[sp_BlitzBackups] TO username;
GRANT EXECUTE ON [dbo].[sp_BlitzLock] TO username;
GRANT EXECUTE ON [dbo].[sp_BlitzWho] TO username;
GRANT EXECUTE ON [dbo].[sp_DatabaseRestore] TO username;
GRANT EXECUTE ON [dbo].[sp_ineachdb] TO username;
GO

USE master
GO

GRANT VIEW SERVER STATE TO username;
```

## Extra Directories and Files

You need to create/prepare the following directories and files that are not part of the github repository:
- `db` directory - containing the knowledge database used by the agent
- `.env` file - containing the environment variables

## OpenAI API Key

You need to create an OpenAI API key and add it to the `.env` file as `OPENAI_API_KEY`.

## Basic Authentication

To secure the application with basic authentication, create an `.htpasswd` file:

```sh
docker run --rm -it httpd:2.4-alpine htpasswd -nbB admin 'TvojeSilneHeslo' > .htpasswd
```

This creates a password file with username `admin` and password `TvojeSilneHeslo`. You can change these credentials as needed.

# Usage

Docker needs to be installed on your system. Once everything is prepared, you can build the image and run the container:

```sh
docker-compose up --build
```

The application will be available at `http://localhost:8080` with basic authentication. Use the credentials you created in the `.htpasswd` file (by default: username `admin`, password `TvojeSilneHeslo`).
