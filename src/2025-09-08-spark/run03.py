from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys


def create_spark_session(app_name="LocationEventsAggregation"):
    """Create and configure Spark session with memory optimizations"""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
        .getOrCreate()
    )


def read_csv_with_auto_schema(spark, input_path):
    """Read CSV and let Spark infer the schema, then inspect what we have"""

    # First, read with header=false to see the raw structure
    df_raw = (
        spark.read.option("delimiter", "\t")
        .option("header", "false")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    return df_raw


def create_proper_column_names(df):
    """Map the auto-generated column names to proper locationEvents column names"""

    # Based on the actual field order you provided
    column_mapping = [
        "ts",
        "sessionId",
        "osId",
        "advertisingId",
        "sourceIp",
        "country",
        "publisherId",
        "productId",
        "eDate",
        "cityId",
        "localTs",
        "longitude",
        "latitude",
        "ispNameId",
        "networkTypeId",
        "simMCC",
        "simMNC",
        "userAgent",
        "sourceModuleId",
        "sourceServerId",
        "isCoppaApp",
        "regionId",
        "publisherTypeId",
        "userId",
        "eventTypeId",
        "consumerId",
        "packageId",
        "limitAdTracking",
        "isValidAdvertisingId",
        "isValidGps",
        "isValidBssidInt",
        "rtbCarrierNameId",
        "rtbCarrierMcc",
        "rtbCarrierMnc",
        "pas",
        "channelId",
        "processingRequestStatus",
        "isQpsSkipped",
        "deviceVersionId",
        "bidFloor",
        "requestId",
        "deviceManufacturerId",
        "deviceModelId",
        "deviceTypeId",
        "gdpr",
        "appNameId",
        "publisherNameId",
        "width",
        "height",
        "genderId",
        "age",
        "languageId",
        "inputLangsId",
        "geoTypeId",
        "advertisingIdSourceId",
        "placementId",
        "videoSupportModeId",
        "nodes",
        "firstNodeId",
        "releaseVersionId",
        "usPrivacyId",
        "dataCenterId",
        "sChainComplete",
        "displayManagerNameId",
        "displayManagerVersionId",
        "omidProviderNameId",
        "omidProviderVersionId",
        "subPublisherManagementTypeId",
        "isVisibility",
        "localeId",
        "locationTs",
        "adTagId",
        "sitePageId",
        "siteNameId",
        "siteDomainId",
        "trafficTypeId",
        "sioIdTypeId",
        "isUserId",
    ]

    # Get current column names
    current_columns = df.columns

    # Create select expression with renamed columns
    select_exprs = []
    for i, col_name in enumerate(current_columns):
        if i < len(column_mapping):
            new_name = column_mapping[i]
            select_exprs.append(col(col_name).alias(new_name))
        else:
            # If we have more columns than expected, name them as extra_N
            select_exprs.append(col(col_name).alias(f"extra_{i}"))

    return df.select(*select_exprs)


def compute_user_id_from_advertising_id(df, output_digits=8):
    """
    Compute userId deterministically from advertisingId using xxhash64.
    """
    mod = 10 ** int(output_digits)
    hashed = abs(xxhash64(col("advertisingId")).cast("long"))
    user_id_col = (
        when(col("advertisingId").isNull() | (length(trim(col("advertisingId"))) == 0), lit(None))
        .otherwise((hashed % mod).cast("long"))
    )
    return df.withColumn("userId", user_id_col)


def compute_source_ip_as_int(df):
    """
    Convert sourceIp (IPv4 dotted string) into a 32-bit unsigned integer value.
    Implemented purely with Spark SQL functions (no ipToLong dependency).
    - Valid IPv4 -> numeric BIGINT in [0, 4294967295]
    - Invalid/NULL -> NULL
    """
    parts = split(col("sourceIp"), "\.")
    p0 = parts.getItem(0).cast("int")
    p1 = parts.getItem(1).cast("int")
    p2 = parts.getItem(2).cast("int")
    p3 = parts.getItem(3).cast("int")

    valid_pattern = col("sourceIp").rlike(r"^(?:\d{1,3}\.){3}\d{1,3}$")
    valid_range = (
        p0.between(0, 255) & p1.between(0, 255) & p2.between(0, 255) & p3.between(0, 255)
    )

    # Build 32-bit integer: a*256^3 + b*256^2 + c*256 + d
    p0l = p0.cast("bigint")
    p1l = p1.cast("bigint")
    p2l = p2.cast("bigint")
    p3l = p3.cast("bigint")

    ip_num = p0l * lit(16777216) + p1l * lit(65536) + p2l * lit(256) + p3l

    return df.withColumn(
        "sourceIp",
        when(valid_pattern & valid_range, ip_num).otherwise(lit(None).cast("bigint")),
    )


def add_derived_columns(df):
    """Add derived columns needed for the aggregation (assumes userId already exists)."""
    return (
        df.withColumn("time_slice_minute", date_trunc("minute", col("ts")))
        .withColumn("numBidRequests", when(col("eventTypeId") == 102, 1).otherwise(0))
        .withColumn("numEvents", lit(1))
        .repartition(200, "userId", "time_slice_minute")  # Repartition for better performance
    )


def perform_memory_efficient_aggregation(df, run_id=12345):
    """Perform aggregation with memory optimizations"""

    available_columns = set(df.columns)
    print(f"Available columns: {len(available_columns)} total")

    # Use fewer grouping dimensions to reduce memory pressure
    # Start with essential grouping columns only
    group_cols = []

    if "userId" in available_columns:
        group_cols.append("userId")
    if "time_slice_minute" in available_columns:
        group_cols.append("time_slice_minute")
    if "sourceIp" in available_columns:
        group_cols.append("sourceIp")
    # Remove latitude/longitude from grouping to reduce cardinality
    if "publisherTypeId" in available_columns:
        group_cols.append("publisherTypeId")
    if "packageId" in available_columns:
        group_cols.append("packageId")
    if "productId" in available_columns:
        group_cols.append("productId")
    if "networkTypeId" in available_columns:
        group_cols.append("networkTypeId")

    print(f"Grouping by columns: {group_cols}")

    # Build a more focused set of aggregations
    agg_exprs = []

    # Core aggregations - only the most important ones
    agg_exprs.extend(
        [
            max("ts").alias("ts"),
            max("sessionId").alias("sessionId"),
            min("osId").alias("osId"),
            max("advertisingId").alias("advertisingId"),
            max("country").alias("country"),
            max("publisherId").alias("publisherId"),
            max("eDate").alias("eDate"),
            max("cityId").alias("cityId"),
            max("localTs").alias("localTs"),
            max("ispNameId").alias("ispNameId"),
            max("userAgent").alias("userAgent"),
            max("sourceModuleId").alias("sourceModuleId"),
            max("sourceServerId").alias("sourceServerId"),
            max("isCoppaApp").alias("isCoppaApp"),
            max("regionId").alias("regionId"),
            max("eventTypeId").alias("eventTypeId"),
            max("consumerId").alias("consumerId"),
            max("isValidAdvertisingId").alias("isValidAdvertisingId"),
            # Location data as aggregated values since not in groupBy
            max("latitude").alias("latitude"),
            max("longitude").alias("longitude"),
            # Timestamps and IDs
            current_timestamp().alias("loadTs"),
            lit(run_id).alias("biRunId"),
            # Count aggregations
            sum(when(col("eventTypeId") == 102, 1).otherwise(0)).alias("numBidRequests"),
            count("*").alias("numEvents"),
        ]
    )

    # Add simple NULL placeholders for missing target schema columns
    missing_columns = [
        "ispId",
        "localTZoneId",
        "locationSourceId",
        "locationAccuracy",
        "locationTs",
        "simMCC",
        "simMNC",
        "cellMCC",
        "cellMNC",
        "cellLAC",
        "cellId",
        "cellSignalLevel",
        "ssid",
        "bssid",
        "signalLevel",
        "limitAdTracking",
        "isValidGps",
        "pas",
        "channelId",
    ]

    for col_name in missing_columns:
        agg_exprs.append(lit(None).cast("string").alias(col_name))

    print("Starting aggregation with reduced dimensions...")

    # Cache the filtered data before aggregation
    df.cache()
    row_count = df.count()
    print(f"Cached {row_count} rows for aggregation")

    if group_cols:
        result_df = df.groupBy(*group_cols).agg(*agg_exprs)
    else:
        result_df = df.agg(*agg_exprs)

    # Immediately persist the result to avoid recomputation
    result_df = result_df.persist()

    return result_df


def fix_void_columns(df):
    """Fix any VOID type columns that cause CSV write issues"""

    # Get the schema and identify VOID columns
    schema = df.schema
    select_exprs = []

    for field in schema.fields:
        col_name = field.name
        data_type = field.dataType

        if str(data_type) == "VoidType" or "void" in str(data_type).lower():
            # Cast VOID columns to StringType with NULL
            select_exprs.append(lit(None).cast("string").alias(col_name))
            print(f"Fixed VOID column: {col_name}")
        else:
            # Keep the original column
            select_exprs.append(col(col_name))

    return df.select(*select_exprs)


def apply_filters(df):
    """Apply basic filters"""

    filtered_df = df

    # Apply filters only if columns exist
    if "isValidAdvertisingId" in df.columns:
        filtered_df = filtered_df.filter(col("isValidAdvertisingId") == 1)

    if "publisherTypeId" in df.columns:
        # Example publisher type filter
        filtered_df = filtered_df.filter(col("publisherTypeId").isin([1, 2, 7]))

    if "country" in df.columns:
        # Example country filter
        filtered_df = filtered_df.filter(col("country").isin(["US"]))

    return filtered_df


def main(input_path, output_path, run_id=12345):
    """Main function to run the PySpark job"""

    # Create Spark session
    spark = create_spark_session()

    try:
        # Read the CSV data
        print(f"Reading data from {input_path}")
        df_raw = read_csv_with_auto_schema(spark, input_path)

        print(f"Raw data has {df_raw.count()} rows and {len(df_raw.columns)} columns")
        print("Raw column names:", df_raw.columns[:10], "..." if len(df_raw.columns) > 10 else "")

        # Map to proper column names
        df = create_proper_column_names(df_raw)

        print("Mapped column names:", df.columns[:10], "..." if len(df.columns) > 10 else "")

        # Derive userId from advertisingId **before** any repartitioning or grouping
        # Derive userId from advertisingId **before** any repartitioning or grouping
        df = compute_user_id_from_advertising_id(df, output_digits=hash_digits)

        # Convert sourceIp to integer (BIGINT) early so grouping uses the numeric form
        df = compute_source_ip_as_int(df)

        # Show sample data
        print("Sample data (with computed userId and numeric sourceIp):")
        df.select("advertisingId", "userId", "sourceIp").show(5, truncate=False)

        # Add derived columns (repartitions by userId)
        df = add_derived_columns(df)

        # Apply filters
        df = apply_filters(df)

        print(f"After filtering: {df.count()} rows")

        # Perform aggregation
        print("Performing memory-efficient aggregation...")
        aggregated_df = perform_memory_efficient_aggregation(df, run_id)

        # Show sample results
        print("Sample aggregated results:")
        aggregated_df.select("userId", "time_slice_minute", "numEvents", "numBidRequests").show(
            5, truncate=False
        )

        print(f"Aggregated data has {aggregated_df.count()} rows")

        # Fix any VOID type columns before writing
        print("Checking for VOID type columns...")
        aggregated_df.printSchema()

        # Convert all columns to string to avoid VOID type issues
        print("Converting all columns to string type for safe CSV export...")
        string_columns = []
        for col_name in aggregated_df.columns:
            string_columns.append(col(col_name).cast("string").alias(col_name))

        safe_df = aggregated_df.select(*string_columns)

        # Write results
        print(f"Writing results to {output_path}")

        # Write with error handling for problematic columns
        try:
            (
                safe_df.coalesce(1)
                .write.mode("overwrite")
                .option("header", "true")
                .option("nullValue", "")
                .csv(output_path)
            )
            print("Successfully wrote CSV output!")
        except Exception as write_error:
            print(f"CSV write failed: {write_error}")
            print("Attempting to write as Parquet instead...")

            # Try original dataframe as Parquet (might work better)
            try:
                (
                    safe_df.coalesce(1)
                    .write.mode("overwrite")
                    .parquet(output_path + "_parquet")
                )
                print(f"Successfully wrote as Parquet to {output_path}_parquet")
            except Exception as parquet_error:
                print(f"Parquet write also failed: {parquet_error}")
                print("Writing just the core columns...")

                # Write only core non-problematic columns
                core_columns = [
                    "userId",
                    "time_slice_minute",
                    "sourceIp",
                    "publisherTypeId",
                    "packageId",
                    "productId",
                    "networkTypeId",
                    "ts",
                    "sessionId",
                    "country",
                    "numBidRequests",
                    "numEvents",
                ]

                core_df = safe_df.select(
                    *[col(c) for c in core_columns if c in safe_df.columns]
                )
                (
                    core_df.coalesce(1)
                    .write.mode("overwrite")
                    .option("header", "true")
                    .csv(output_path + "_core")
                )

        print("Job completed successfully!")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback

        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python script.py <input_path> <output_path> [run_id] [hash_digits]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    run_id = int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3].isdigit() else 12345
    hash_digits = int(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4].isdigit() else 8

    main(input_path, output_path, run_id)
