-- ==========================================
-- EXTENSIONS: SQL SERVER SERVICE BROKER
-- ==========================================
-- Run this script AFTER ssf-sqlserver-minimal.sql to enable Service Broker signaling.
-- It defines the necessary message types, contracts, queues, and services, 
-- and overrides the stub notify function to send real messages including tenant information.

IF NOT EXISTS (SELECT 1 FROM sys.service_message_types WHERE name = 'ssf_NotificationMessage')
BEGIN
    CREATE MESSAGE TYPE [ssf_NotificationMessage] VALIDATION = NONE;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.service_contracts WHERE name = 'ssf_NotificationContract')
BEGIN
    CREATE CONTRACT [ssf_NotificationContract] ([ssf_NotificationMessage] SENT BY INITIATOR);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.service_queues WHERE name = 'NotificationQueue' AND schema_id = SCHEMA_ID('ssf'))
BEGIN
    CREATE QUEUE ssf.NotificationQueue;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.services WHERE name = 'ssf_NotificationService')
BEGIN
    CREATE SERVICE [ssf_NotificationService] ON QUEUE ssf.NotificationQueue ([ssf_NotificationContract]);
END
GO

CREATE OR ALTER PROCEDURE ssf.notify_workers
    @p_tenant_id NVARCHAR(128),
    @p_queue_name NVARCHAR(57),
    @p_event NVARCHAR(50) = 'event'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @dlg UNIQUEIDENTIFIER;
    
    -- Constructing JSON payload including tenant_id
    DECLARE @msg NVARCHAR(MAX) = N'{"tenant_id":"' + STRING_ESCAPE(@p_tenant_id, 'json') + 
                                 N'","queue":"' + STRING_ESCAPE(@p_queue_name, 'json') + 
                                 N'","event":"' + STRING_ESCAPE(@p_event, 'json') + N'"}';

    BEGIN DIALOG @dlg
        FROM SERVICE [ssf_NotificationService]
        TO SERVICE 'ssf_NotificationService'
        ON CONTRACT [ssf_NotificationContract]
        WITH ENCRYPTION = OFF;

    SEND ON CONVERSATION @dlg MESSAGE TYPE [ssf_NotificationMessage] (@msg);
    END CONVERSATION @dlg;
END;
GO