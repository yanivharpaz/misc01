from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session(app_name="LocationEventsAggregation"):
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def define_schema():
    """Define the schema for the locationEvents table based on the SQL CREATE TABLE"""
    return StructType([
        StructField("ts", TimestampType(), True),
        StructField("sessionId", StringType(), True),  # Using String as the sample shows mixed types
        StructField("osId", IntegerType(), True),
        StructField("advertisingId", StringType(), True),
        StructField("sourceIp", StringType(), True),  # IP addresses as strings
        StructField("country", StringType(), True),
        StructField("ispId", LongType(), True),
        StructField("publisherId", LongType(), True),
        StructField("eDate", DateType(), True),
        StructField("cityId", IntegerType(), True),
        StructField("localTs", TimestampType(), True),
        StructField("localTZoneId", DoubleType(), True),  # Timezone offset
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("locationAccuracy", IntegerType(), True),
        StructField("locationSourceId", IntegerType(), True),
        StructField("locationTs", StringType(), True),  # Empty in sample, keeping as string
        StructField("userAgent", StringType(), True),
        StructField("sdkTypeId", IntegerType(), True),
        StructField("sourceModuleId", LongType(), True),
        StructField("sourceServerId", IntegerType(), True),
        StructField("isCoppaApp", IntegerType(), True),
        StructField("locationContextId", IntegerType(), True),
        StructField("advertisingId2", StringType(), True),  # Second advertising ID field
        StructField("eventTypeId", IntegerType(), True),
        StructField("publisherTypeId", LongType(), True),
        StructField("regionId", IntegerType(), True),
        StructField("mRegistered1", IntegerType(), True),
        StructField("cellMcc1", IntegerType(), True),
        StructField("cellMnc1", IntegerType(), True),
        StructField("cellLac1", IntegerType(), True),
        StructField("packageId", IntegerType(), True),
        StructField("simMCC", IntegerType(), True),
        StructField("simMNC", StringType(), True),
        StructField("cellMCC", IntegerType(), True),
        StructField("cellId1", LongType(), True),
        StructField("cellSignalLevel1", IntegerType(), True),
        StructField("ssid", StringType(), True),
        StructField("cellLAC", IntegerType(), True),
        StructField("locationAccuracy1", DoubleType(), True),
        StructField("ssid1", StringType(), True),
        StructField("userId", LongType(), True),
        StructField("ispNameId", LongType(), True),
        StructField("networkTypeId", IntegerType(), True),
        StructField("isValidAdvertisingId", IntegerType(), True),
        StructField("bssid", LongType(), True),
        StructField("ssid2", StringType(), True),
        StructField("wifiRssiLevel", IntegerType(), True),
        StructField("wifiRssiLevel1", IntegerType(), True),
        StructField("ssid3", StringType(), True),
        StructField("ssid4", StringType(), True),
        StructField("consumerId", LongType(), True),
        StructField("ssid5", StringType(), True),
        StructField("sdkVersionId", IntegerType(), True),
        StructField("age", IntegerType(), True),
        StructField("deviceManufacturerId", IntegerType(), True),
        StructField("genderId", IntegerType(), True),
        StructField("deviceModelId", IntegerType(), True),
        StructField("appNameId", LongType(), True),
        StructField("deviceTypeId", LongType(), True),
        StructField("deviceVersionId", LongType(), True),
        StructField("channelId", LongType(), True),
        StructField("isLocationEnabled", IntegerType(), True),
        StructField("permissions", StringType(), True),
        StructField("gdpr", StringType(), True),
        StructField("usPrivacyId", StringType(), True),
        StructField("isValidGps", IntegerType(), True),
        StructField("limitAdTracking", StringType(), True),
        StructField("pas", IntegerType(), True),
        StructField("cellId", IntegerType(), True),
        StructField("advertisingIdSourceId", StringType(), True),
        StructField("rawClientTS", LongType(), True),
        StructField("bssid1", StringType(), True),
        StructField("bssid2", StringType(), True),
        StructField("bssid3", StringType(), True),
        StructField("isValidBssidInt", IntegerType(), True),
        StructField("bssid4", StringType(), True),
        StructField("bssid5", IntegerType(), True)
    ])

def read_csv_data(spark, input_path):
    """Read CSV data with the defined schema"""
    schema = define_schema()
    
    # Read the CSV file - it appears to be tab-delimited based on the sample
    df = spark.read \
        .option("delimiter", "\t") \
        .option("header", "false") \
        .option("quote", "") \
        .option("escape", "") \
        .option("multiLine", "true") \
        .schema(schema) \
        .csv(input_path)
    
    return df

def add_derived_columns(df):
    """Add derived columns needed for the aggregation"""
    return df.withColumn("time_slice_minute", 
                        date_trunc("minute", col("ts"))) \
            .withColumn("numBidRequests", 
                       when(col("eventTypeId") == 102, 1).otherwise(0)) \
            .withColumn("numEvents", lit(1))

def perform_aggregation(df, run_id=12345):
    """Perform the aggregation as specified in the SQL query"""
    
    # Group by the specified columns
    grouped_df = df.groupBy(
        "userId",
        "time_slice_minute", 
        "sourceIp",
        "latitude", 
        "longitude",
        "publisherTypeId",
        "packageId", 
        "productId",  # Assuming this maps to one of the ID fields
        "bssid",
        "networkTypeId"
    ).agg(
        # Timestamp and session info
        max("ts").alias("ts"),
        max("sessionId").alias("sessionId"),
        min("osId").alias("osId"),
        max("advertisingId").alias("advertisingId"),
        first("sourceIp").alias("sourceIp"),
        max("country").alias("country"),
        max("ispId").alias("ispId"),
        max("publisherId").alias("publisherId"),
        first("productId").alias("productId"),  # Assuming productId is one of the fields
        max("eDate").alias("eDate"),
        max("cityId").alias("cityId"),
        max("localTs").alias("localTs"),
        max("localTZoneId").alias("localTZoneId"),
        max("locationSourceId").alias("locationSourceId"),
        first("longitude").alias("longitude"),
        first("latitude").alias("latitude"),
        min("locationAccuracy").alias("locationAccuracy"),
        max("locationTs").alias("locationTs"),
        max("ispNameId").alias("ispNameId"),
        first("networkTypeId").alias("networkTypeId"),
        
        # Cellular info with NULLIF logic
        when(max("simMCC") == -9999, lit(None)).otherwise(max("simMCC")).alias("simMCC"),
        when(max("simMNC") == "-9999", lit(None)).otherwise(max("simMNC")).alias("simMNC"),
        when(max("cellMCC") == -9999, lit(None)).otherwise(max("cellMCC")).alias("cellMCC"),
        when(max("cellMnc1") == -9999, lit(None)).otherwise(max("cellMnc1")).alias("cellMNC"),
        max("cellLAC").alias("cellLAC"),
        max("cellId").alias("cellId"),
        min("cellSignalLevel1").alias("cellSignalLevel"),
        
        # WiFi info
        max("ssid").alias("ssid"),
        when(first("bssid") == -1, lit(None)).otherwise(first("bssid")).alias("bssid"),
        min("wifiRssiLevel").alias("signalLevel"),
        max("ssid1").alias("ssid1"),
        when(max("bssid1") == "-1", lit(None)).otherwise(max("bssid1")).alias("bssid1"),
        min("wifiRssiLevel1").alias("signalLevel1"),
        max("locationTs").alias("scanTs1"),  # Assuming scanTs maps to locationTs
        max("ssid2").alias("ssid2"),
        when(max("bssid2") == "-1", lit(None)).otherwise(max("bssid2")).alias("bssid2"),
        min("wifiRssiLevel").alias("signalLevel2"),
        max("locationTs").alias("scanTs2"),
        max("ssid3").alias("ssid3"),
        when(max("bssid3") == "-1", lit(None)).otherwise(max("bssid3")).alias("bssid3"),
        min("wifiRssiLevel").alias("signalLevel3"),
        max("locationTs").alias("scanTs3"),
        max("ssid4").alias("ssid4"),
        when(max("bssid4") == "-1", lit(None)).otherwise(max("bssid4")).alias("bssid4"),
        min("wifiRssiLevel").alias("signalLevel4"),
        max("locationTs").alias("scanTs4"),
        max("ssid5").alias("ssid5"),
        when(max("bssid5") == -1, lit(None)).otherwise(max("bssid5")).alias("bssid5"),
        min("wifiRssiLevel").alias("signalLevel5"),
        max("locationTs").alias("scanTs5"),
        
        # Device and app info
        max("userAgent").alias("userAgent"),
        max("sdkTypeId").alias("sdkTypeId"),
        max("sourceModuleId").alias("sourceModuleId"),
        max("sourceServerId").alias("sourceServerId"),
        max("isCoppaApp").alias("isCoppaApp"),
        max("locationContextId").alias("locationContextId"),
        max("regionId").alias("regionId"),
        first("publisherTypeId").alias("publisherTypeId"),
        
        # Additional cellular towers (1-5)
        max("mRegistered1").alias("mRegistered1"),
        max("cellMcc1").alias("cellMcc1"),
        max("cellMnc1").alias("cellMnc1"),
        max("cellLac1").alias("cellLac1"),
        max("cellId1").alias("cellId1"),
        max(lit(None)).alias("cellUnit1"),  # Not in source data
        max(lit(None)).alias("cellBlon1"),  # Not in source data
        max(lit(None)).alias("cellBlat1"),  # Not in source data
        max(lit(None)).alias("cellRadio1"), # Not in source data
        max("locationTs").alias("cellTs1"),
        min("cellSignalLevel1").alias("cellSignalLevel1"),
        
        # Repeat for cells 2-5 (setting to NULL as not clearly mapped in source)
        max(lit(None)).alias("mRegistered2"),
        max(lit(None)).alias("cellMcc2"),
        max(lit(None)).alias("cellMnc2"),
        max(lit(None)).alias("cellLac2"),
        max(lit(None)).alias("cellId2"),
        max(lit(None)).alias("cellUnit2"),
        max(lit(None)).alias("cellBlon2"),
        max(lit(None)).alias("cellBlat2"),
        max(lit(None)).alias("cellRadio2"),
        max(lit(None)).alias("cellTs2"),
        min(lit(None)).alias("cellSignalLevel2"),
        
        max(lit(None)).alias("mRegistered3"),
        max(lit(None)).alias("cellMcc3"),
        max(lit(None)).alias("cellMnc3"),
        max(lit(None)).alias("cellLac3"),
        max(lit(None)).alias("cellId3"),
        max(lit(None)).alias("cellUnit3"),
        max(lit(None)).alias("cellBlon3"),
        max(lit(None)).alias("cellBlat3"),
        max(lit(None)).alias("cellRadio3"),
        max(lit(None)).alias("cellTs3"),
        min(lit(None)).alias("cellSignalLevel3"),
        
        max(lit(None)).alias("mRegistered4"),
        max(lit(None)).alias("cellMcc4"),
        max(lit(None)).alias("cellMnc4"),
        max(lit(None)).alias("cellLac4"),
        max(lit(None)).alias("cellId4"),
        max(lit(None)).alias("cellUnit4"),
        max(lit(None)).alias("cellBlon4"),
        max(lit(None)).alias("cellBlat4"),
        max(lit(None)).alias("cellRadio4"),
        max(lit(None)).alias("cellTs4"),
        min(lit(None)).alias("cellSignalLevel4"),
        
        max(lit(None)).alias("mRegistered5"),
        max(lit(None)).alias("cellMcc5"),
        max(lit(None)).alias("cellMnc5"),
        max(lit(None)).alias("cellLac5"),
        max(lit(None)).alias("cellId5"),
        max(lit(None)).alias("cellUnit5"),
        max(lit(None)).alias("cellBlon5"),
        max(lit(None)).alias("cellBlat5"),
        max(lit(None)).alias("cellRadio5"),
        max(lit(None)).alias("cellTs5"),
        min(lit(None)).alias("cellSignalLevel5"),
        
        # Additional location data (1-3)
        max(lit(None)).alias("locationSourceId1"),
        max(lit(None)).alias("longitude1"),
        max(lit(None)).alias("latitude1"),
        max("locationAccuracy1").alias("locationAccuracy1"),
        max("locationTs").alias("locationTs1"),
        max(lit(None)).alias("locationSourceId2"),
        max(lit(None)).alias("longitude2"),
        max(lit(None)).alias("latitude2"),
        max(lit(None)).alias("locationAccuracy2"),
        max(lit(None)).alias("locationTs2"),
        max(lit(None)).alias("locationSourceId3"),
        max(lit(None)).alias("longitude3"),
        max(lit(None)).alias("latitude3"),
        max(lit(None)).alias("locationAccuracy3"),
        max(lit(None)).alias("locationTs3"),
        
        # Enriched location data
        max(lit(None)).alias("enrichedLatitude"),
        max(lit(None)).alias("enrichedLongitude"),
        max(lit(None)).alias("enrichedAccuracy"),
        max(lit(None)).alias("enrichedLocationTs"),
        
        # User and device info
        first("userId").alias("userId"),
        max(lit(None)).alias("rawIspId"),
        max(lit(None)).alias("rawIspNameId"),
        max("eventTypeId").alias("eventTypeId"),
        max("wifiRssiLevel").alias("wifiRssiLevel"),
        max(lit(None)).alias("cellIspNameId"),
        max(lit(None)).alias("isRoaming"),
        max(lit(None)).alias("isForeground"),
        max("wifiRssiLevel1").alias("wifiRssiLevel1"),
        max(lit(None)).alias("wifiRssiLevel2"),
        max(lit(None)).alias("wifiRssiLevel3"),
        max(lit(None)).alias("wifiRssiLevel4"),
        max(lit(None)).alias("wifiRssiLevel5"),
        max("sdkVersionId").alias("sdkVersionId"),
        
        # System timestamp
        current_timestamp().alias("loadTs"),
        max("consumerId").alias("consumerId"),
        max("rawClientTS").alias("rawClientTS"),
        first("packageId").alias("packageId"),
        max("pas").alias("pas"),
        max("limitAdTracking").alias("limitAdTracking"),
        max("isValidAdvertisingId").alias("isValidAdvertisingId"),
        max("isValidGps").alias("isValidGps"),
        max("isValidBssidInt").alias("isValidBssidInt"),
        max(lit(None)).alias("rtbCarrierNameId"),
        max(lit(None)).alias("rtbCarrierMcc"),
        max(lit(None)).alias("rtbCarrierMnc"),
        
        # Run ID
        lit(run_id).alias("biRunId"),
        max("advertisingIdSourceId").alias("advertisingIdSourceId"),
        max("channelId").alias("channelId"),
        max("gdpr").alias("gdpr"),
        max("permissions").alias("permissions"),
        max("isLocationEnabled").alias("isLocationEnabled"),
        max("deviceManufacturerId").alias("deviceManufacturerId"),
        max("deviceModelId").alias("deviceModelId"),
        max("deviceTypeId").alias("deviceTypeId"),
        max("deviceVersionId").alias("deviceVersionId"),
        max("genderId").alias("genderId"),
        max("age").alias("age"),
        max(lit(None)).alias("inputLangsId"),
        max(lit(None)).alias("geoTypeId"),
        max("usPrivacyId").alias("usPrivacyId"),
        max(lit(None)).alias("localeId"),
        max(lit(None)).alias("trafficTypeId"),
        max(lit(None)).alias("sioIdTypeId"),
        max(lit(None)).alias("siteDomainId"),
        max(lit(None)).alias("sitePageId"),
        max(lit(None)).alias("siteNameId"),
        
        # Aggregated metrics
        sum(when(col("eventTypeId") == 102, 1).otherwise(0)).alias("numBidRequests"),
        count("*").alias("numEvents"),
        max("appNameId").alias("appNameId")
    )
    
    return grouped_df

def apply_filters(df, load_ts_from=None, load_ts_to=None, max_days_back=30, 
                 publisher_type_ids=None, countries=None):
    """Apply filters similar to the WHERE clause in the SQL"""
    
    filtered_df = df.filter(col("isValidAdvertisingId") == 1)
    
    if load_ts_from:
        filtered_df = filtered_df.filter(col("eDate") >= date_sub(lit(load_ts_from).cast("date"), max_days_back))
    
    if publisher_type_ids:
        filtered_df = filtered_df.filter(col("publisherTypeId").isin(publisher_type_ids))
    
    if countries:
        filtered_df = filtered_df.filter(col("country").isin(countries))
    
    # Filter out users to delete for GDPR (this would need to be joined with actual GDPR table)
    # filtered_df = filtered_df.filter(col("userId").isNotNull())
    
    return filtered_df

def main(input_path, output_path, run_id=12345):
    """Main function to run the PySpark job"""
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read the CSV data
        print(f"Reading data from {input_path}")
        df = read_csv_data(spark, input_path)
        
        # Add derived columns
        df = add_derived_columns(df)
        
        # Apply filters (customize these parameters as needed)
        df = apply_filters(df, 
                          publisher_type_ids=[1, 2, 7],  # Example values
                          countries=["US"])  # Example filter
        
        # Perform aggregation
        print("Performing aggregation...")
        aggregated_df = perform_aggregation(df, run_id)
        
        # Show sample results
        print("Sample results:")
        aggregated_df.show(5, truncate=False)
        
        # Write results
        print(f"Writing results to {output_path}")
        aggregated_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        print("Job completed successfully!")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python script.py <input_path> <output_path> [run_id]")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    run_id = int(sys.argv[3]) if len(sys.argv) > 3 else 12345
    
    main(input_path, output_path, run_id)