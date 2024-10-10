CREATE PROCEDURE [etl].[UpdateFactOrders]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @sprocStep VARCHAR(200), @startStamp DATETIME, @endStamp DATETIME;

    BEGIN TRY
        -- Get start date/time
        SET @sprocStep = 'Get start date/time';
        SET @startStamp = GETDATE();

        -- Set minimum depart date limit for performance optimization
        SET @sprocStep = 'Set min depart date limit';
        DECLARE @updateDateLimit DATE = DATEADD(DAY, -5, GETDATE());

        -- Clear order fact updates cache if exists
        SET @sprocStep = 'Clear order fact updates cache';
        IF (OBJECT_ID('stage.order_fact_updates') IS NOT NULL)
            TRUNCATE TABLE stage.order_fact_updates;

        -- Build order fact updates cache
        SET @sprocStep = 'Build order fact updates cache';
        INSERT INTO stage.order_fact_updates (order_id, order_updated, order_confirmed, order_cancelled, order_due, order_moved, order_autoinvoice_collected, order_nights, order_late, order_non_delivery)
        SELECT o.order_id, o.order_updated, o.order_confirmed, o.order_cancelled, o.order_due, o.order_moved, o.order_autoinvoice_collected, o.order_nights, o.order_late, CAST(0 AS BIT)
        FROM stage.traveller_order_facts o
        WHERE (o.order_depart >= @updateDateLimit)
           OR (DATEDIFF(HOUR, o.order_updated, GETDATE()) <= 12)
           OR (o.order_end >= @updateDateLimit)
           OR (NOT EXISTS (SELECT 1 FROM fact.orders fo WHERE fo.order_id = o.order_id));

        -- Set non-delivery flag
        SET @sprocStep = 'Set non-delivery flag';
        UPDATE o
        SET o.order_non_delivery = 1
        FROM stage.order_fact_updates o
        JOIN dim.Feature f ON f.LinkID = o.order_id
        WHERE f.Type = 36 AND f.SID = 4 AND f.AID = 164; -- Type: 36 = Order, SID: 4  = Order Confirmed, AID: 164 = Not Delivered

        -- Merge into fact.orders
        SET @sprocStep = 'Run fact.orders merge statement';
        MERGE fact.orders fo
        USING stage.order_fact_updates o ON fo.order_id = o.order_id
        WHEN MATCHED THEN 
            UPDATE 
            SET fo.order_updated = o.order_updated,
                fo.order_confirmed = o.order_confirmed,
                fo.order_cancelled = o.order_cancelled,
                fo.order_due = o.order_due,
                fo.order_moved = o.order_moved,
                fo.order_autoinvoice_collected = o.order_autoinvoice_collected,
                fo.order_nights = o.order_nights,
                fo.order_late = o.order_late,
                fo.order_non_delivery = o.order_non_delivery
        WHEN NOT MATCHED THEN
            INSERT (order_id, order_updated, order_confirmed, order_cancelled, order_due, order_moved, order_autoinvoice_collected, order_nights, order_late, order_non_delivery)
            VALUES (o.order_id, o.order_updated, o.order_confirmed, o.order_cancelled, o.order_due, o.order_moved, o.order_autoinvoice_collected, o.order_nights, o.order_late, o.order_non_delivery);

        -- Set end date/time and log performance
        SET @endStamp = GETDATE();
        EXEC [log].updateSprocPerformanceLog @@PROCID, @startStamp, @endStamp;

    END TRY
    BEGIN CATCH
        DECLARE @errorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @sprocName NVARCHAR(50) = 'etl.UpdateFactOrders';

        -- Log exception details
        INSERT INTO [log].exceptions (excep_message, excep_severity, excep_state, excep_object, excep_reference, excep_datetime)
        VALUES (@errorMessage, ERROR_SEVERITY(), ERROR_STATE(), @sprocName, @sprocStep, GETDATE());

        -- Rethrow error
        THROW;
    END CATCH
END;
GO
