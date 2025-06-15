# AI Agent Based on Brent Ozar's First Responder Scripts

This is a AI agent that connects to your SQL Server instance and helps with database administration tasks. 
It is a web application that runs in a Docker container and uses OpenAI LLM API.

# Preparation

## Brent Ozar First Responder Kit

This application connects to a SQL Server instance and runs [Brent Ozar's First Responder scripts](https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit).
Therefore, you need to have the scripts installed on your SQL Server instance.

## SQL Server Database Permissions

You need a `username` to your SQL Server database with the following permissions:
```sql
USE dbname;
GO

GRANT VIEW DATABASE STATE TO username;
GRANT VIEW DEFINITION TO username;
GRANT EXECUTE ON [dbo].[sp_BlitzFirst] TO username;
GRANT EXECUTE ON [dbo].[sp_BlitzIndex] TO username;
GRANT EXECUTE ON [dbo].[sp_BlitzCache] TO username;
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

# Usage

Docker needs to be installed on your system. Once everything is prepared you can simply build the image and run the container:

```sh
docker-compose up --build
```