USE WideWorldImporters;
GO
exec qpi.clear_db_queries;

select *
from qpi.db_queries;

exec qpi.force
		@query_id = 1, 
		@hints = 'HASH JOIN';	

select *
from qpi.db_forced_queries;

exec qpi.force
		@query_id = 1, 
		@hints = 'MERGE JOIN'
go
select *
from qpi.db_forced_queries;
go
	
exec qpi.force
		@query_id = 1, 
		@hints = N'TABLE HINT(po, INDEX (FK_Purchasing_PurchaseOrders_ContactPersonID))';  
go
select *
from qpi.db_forced_queries;
go


exec qpi.force
		@query_id = 1, 
		@hints = N'TABLE HINT(po, INDEX (FK_Purchasing_PurchaseOrders_SupplierID))';  
go
select *
from qpi.db_forced_queries;
go

SELECT 
	reason, 
	score,
	JSON_VALUE(state, '$.currentValue') state,
	JSON_VALUE(state, '$.reason') state_transition_reason,
    script = JSON_VALUE(details, '$.implementationDetails.script'),
	[current plan_id],
	[recommended plan_id],
	is_revertable_action,
	
	estimated_gain = (regressedPlanExecutionCount+recommendedPlanExecutionCount)
                  *(regressedPlanCpuTimeAverage-recommendedPlanCpuTimeAverage)/1000000
    FROM sys.dm_db_tuning_recommendations
	CROSS APPLY OPENJSON (Details, '$.planForceDetails')
    WITH (  [query_id] int '$.queryId',
            [current plan_id] int '$.regressedPlanId',
            [recommended plan_id] int '$.recommendedPlanId',
            regressedPlanExecutionCount int,
            regressedPlanCpuTimeAverage float,
            recommendedPlanExecutionCount int,
            recommendedPlanCpuTimeAverage float
          ) as planForceDetails;
;