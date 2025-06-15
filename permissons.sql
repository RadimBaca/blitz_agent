-- Necessary permission needed to run First responder kit on your database

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


