-- Step 1: Create a masking policy for the salary field
CREATE OR REPLACE MASKING POLICY salary_masking_policy 
AS (val NUMBER) 
RETURNS NUMBER ->
    CASE
        WHEN CURRENT_ROLE() IN ('LEAD_DEVELOPER') THEN val
        ELSE NULL -- If the user is not using LEAD_DEVELOPER role then the column will be NULL
    END;

-- Step 2: Apply the masking policy to the salary column in the employee table
ALTER TABLE employee 
MODIFY COLUMN salary 
SET MASKING POLICY salary_masking_policy;
