-- Development cleanup only. Do not run in production without reviewing.
DROP TABLE APP_ETL.ORDERS_STG PURGE;
DROP TABLE APP_ETL.ORDERS PURGE;
DROP TABLE APP_ETL.ETL_WATERMARK PURGE;
