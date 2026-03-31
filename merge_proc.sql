CREATE OR REPLACE PROCEDURE SALES_ANALYTICS.CORE.PR_MERGE_PRODUCTS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Execute the MERGE statement
    MERGE INTO SALES_ANALYTICS.CORE.DIM_PRODUCTS AS target
    USING (
        SELECT 
            id AS product_id,
            title,
            price,
            category,
            description,
            rating:rate::FLOAT AS rating_score,
            rating:count::INT AS rating_count
        FROM SALES_ANALYTICS.STAGING.STG_PRODUCTS_RAW
    ) AS source
    ON target.product_id = source.product_id
    
    WHEN MATCHED THEN
        UPDATE SET 
            target.price = source.price,
            target.rating_score = source.rating_score,
            target.rating_count = source.rating_count,
            target.updated_at = CURRENT_TIMESTAMP()
            
    WHEN NOT MATCHED THEN
        INSERT (product_id, title, price, category, description, rating_score, rating_count, created_at)
        VALUES (source.product_id, source.title, source.price, source.category, source.description, source.rating_score, source.rating_count, CURRENT_TIMESTAMP());

    RETURN 'Success: Products merged successfully.';
END;
$$;