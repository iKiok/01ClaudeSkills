# Advanced Polars Patterns

This reference document contains advanced patterns and edge cases for Polars data manipulation.

## Advanced Filtering Patterns

### Complex Boolean Logic

```python
# Combining multiple conditions with precedence
df.filter(
    ((pl.col("age") > 25) & (pl.col("department") == "Sales")) |
    ((pl.col("tenure") > 5) & (pl.col("performance") == "Excellent"))
)

# Using .is_between() for range checks
df.filter(pl.col("salary").is_between(50000, 80000))

# Negation with ~
df.filter(~pl.col("status").is_in(["inactive", "pending"]))
```

### Filtering with Regex

```python
# Email domain filtering
df.filter(pl.col("email").str.contains(r"@(gmail|yahoo)\.com"))

# Extract and filter by pattern
df.filter(
    pl.col("product_code").str.extract(r"^([A-Z]{2})", group_index=1) == "AB"
)
```

## Advanced Aggregation Techniques

### Custom Aggregations

```python
# Multiple statistics per group
df.group_by("category").agg([
    pl.col("value").quantile(0.25).alias("q1"),
    pl.col("value").quantile(0.50).alias("median"),
    pl.col("value").quantile(0.75).alias("q3"),
    (pl.col("value").quantile(0.75) - pl.col("value").quantile(0.25)).alias("iqr")
])
```

### Conditional Aggregations

```python
# Sum only values meeting condition
df.group_by("department").agg([
    pl.col("salary").filter(pl.col("salary") > 50000).sum().alias("high_salary_sum"),
    pl.col("age").filter(pl.col("age") < 30).count().alias("young_employees")
])
```

### Rolling Aggregations

```python
# Time-based rolling window
df.sort("date").with_columns([
    pl.col("sales")
      .rolling_mean(window_size="7d", by="date")
      .alias("7day_rolling_avg"),
    pl.col("sales")
      .rolling_sum(window_size="30d", by="date")
      .alias("30day_rolling_sum")
])
```

## Advanced Join Patterns

### Anti-Join (Find Non-Matches)

```python
# Find records in df1 that don't exist in df2
anti_join = df1.join(df2, on="id", how="anti")
```

### Semi-Join (Filter by Existence)

```python
# Keep only df1 records that have a match in df2
semi_join = df1.join(df2, on="id", how="semi")
```

### Cross Join (Cartesian Product)

```python
# Generate all combinations
cross = df1.join(df2, how="cross")
```

### Join with Suffix Control

```python
# Control suffixes for overlapping column names
result = df1.join(
    df2, 
    on="id", 
    how="left",
    suffix="_right"
)
```

## Advanced Window Functions

### Partitioned Window Operations

```python
# Multiple window functions in one pass
df.with_columns([
    pl.col("salary").rank().over("department").alias("dept_salary_rank"),
    pl.col("sales").mean().over("region").alias("region_avg_sales"),
    pl.col("score").max().over(["department", "team"]).alias("team_max_score")
])
```

### Lead and Lag

```python
# Previous and next values
df.with_columns([
    pl.col("value").shift(1).alias("previous_value"),
    pl.col("value").shift(-1).alias("next_value"),
    (pl.col("value") - pl.col("value").shift(1)).alias("value_change")
])
```

### Expanding Windows

```python
# Cumulative statistics with reset
df.with_columns([
    pl.col("amount").cum_sum().over("account_id").alias("running_balance"),
    pl.col("sales").cum_mean().over("product_id").alias("avg_to_date")
])
```

## Advanced String Manipulation

### Split and Explode

```python
# Split string column and create multiple rows
df.with_columns(
    pl.col("tags").str.split(",")
).explode("tags")

# Split into struct
df.with_columns(
    pl.col("name").str.split_exact(" ", 1).alias("name_parts")
).unnest("name_parts")
```

### String Replacement Patterns

```python
# Multiple replacements
df.with_columns(
    pl.col("text")
      .str.replace_all(r"\s+", " ")  # Collapse whitespace
      .str.replace_all(r"[^\w\s]", "")  # Remove special chars
      .str.strip()
      .alias("cleaned_text")
)
```

### Extract Multiple Groups

```python
# Extract structured data from text
df.with_columns(
    pl.col("log").str.extract_all(r"(\d{4}-\d{2}-\d{2}) (\w+)").alias("extracted")
)
```

## Advanced Date/Time Operations

### Business Day Calculations

```python
# Filter for weekdays only
df.filter(pl.col("date").dt.weekday() < 5)

# Count business days between dates
df.with_columns(
    ((pl.col("end_date") - pl.col("start_date")).dt.days() * 5/7).alias("approx_business_days")
)
```

### Date Binning

```python
# Group by week/month/quarter
df.with_columns([
    pl.col("date").dt.truncate("1w").alias("week_start"),
    pl.col("date").dt.truncate("1mo").alias("month_start"),
    pl.col("date").dt.quarter().alias("quarter")
])
```

### Timezone Handling

```python
# Convert timezones
df.with_columns(
    pl.col("utc_time")
      .dt.convert_time_zone("Europe/Athens")
      .alias("local_time")
)
```

## Advanced Reshaping

### Pivot Operations

```python
# Wide to long format
df.pivot(
    values="sales",
    index="date",
    columns="product",
    aggregate_function="sum"
)
```

### Melt (Long Format)

```python
# Wide to long
df.melt(
    id_vars=["id", "name"],
    value_vars=["q1_sales", "q2_sales", "q3_sales", "q4_sales"],
    variable_name="quarter",
    value_name="sales"
)
```

### Nested Data Structures

```python
# Create nested structures
df.group_by("category").agg([
    pl.struct([
        pl.col("product"),
        pl.col("sales")
    ]).alias("products")
])

# Unnest structures
df.unnest("products")
```

## Memory-Efficient Patterns

### Chunked Processing

```python
# Process large files in chunks
def process_large_file(path: str, chunk_size: int = 10000):
    results = []
    reader = pl.read_csv_batched(path, batch_size=chunk_size)
  
    while True:
        batch = reader.next_batches(1)
        if not batch:
            break
      
        # Process batch
        processed = batch[0].filter(pl.col("value") > 0)
        results.append(processed)
  
    return pl.concat(results)
```

### Streaming Aggregations

```python
# Aggregate without loading full dataset
result = (
    pl.scan_csv("huge_file.csv")
    .group_by("category")
    .agg([
        pl.col("amount").sum(),
        pl.col("quantity").mean()
    ])
    .collect(streaming=True)
)
```

## Type Conversions and Casting

### Complex Type Casting

```python
# Safe casting with error handling
df.with_columns([
    pl.col("string_number").cast(pl.Float64, strict=False).alias("number"),
    pl.col("date_string").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
])

# Cast multiple columns
df.cast({
    "col1": pl.Int32,
    "col2": pl.Float64,
    "col3": pl.Utf8
})
```

### Schema Enforcement

```python
# Define and enforce schema on read
schema = {
    "id": pl.Int64,
    "name": pl.Utf8,
    "amount": pl.Float64,
    "date": pl.Date
}

df = pl.read_csv("data.csv", dtypes=schema)
```

## Performance Optimization Advanced

### Query Optimization Strategies

```python
# View and optimize query plan
lazy_df = pl.scan_csv("data.csv").filter(...).group_by(...)

# Check the optimized plan
print(lazy_df.explain(optimized=True))

# Profile execution
with pl.SQLContext(df=df) as ctx:
    result = ctx.execute("SELECT * FROM df WHERE age > 25")
```

### Parallel Processing

```python
# Polars automatically parallelizes, but you can control threads
import os
os.environ["POLARS_MAX_THREADS"] = "4"

# Or in code
pl.Config.set_streaming_chunk_size(100000)
```

### Column Selection Optimization

```python
# Read only needed columns (lazy)
df = pl.scan_csv("large.csv").select([
    "col1", "col2", "col3"
]).collect()

# Better than reading all columns then selecting
```

## Error Handling Patterns

### Graceful Failure

```python
# Handle potential errors in transformations
df.with_columns([
    pl.col("value")
      .cast(pl.Float64, strict=False)
      .fill_null(0)
      .alias("safe_value")
])
```

### Validation Before Processing

```python
# Check schema before processing
expected_columns = {"id", "name", "value"}
if not expected_columns.issubset(set(df.columns)):
    missing = expected_columns - set(df.columns)
    raise ValueError(f"Missing columns: {missing}")
```

## Integration Patterns

### Pandas Interop (only When absolute Necessary, otherwise clealy avoid using pandas)

```python
# Convert to Pandas for specific library
import matplotlib.pyplot as plt

# Efficient conversion
pandas_df = df.to_pandas()
pandas_df.plot()

# Convert back to Polars
df_back = pl.from_pandas(pandas_df)
```

### Arrow Interop

```python
# Zero-copy to Arrow
arrow_table = df.to_arrow()

# From Arrow
df = pl.from_arrow(arrow_table)
```

### Database Integration

```python
# Read from SQLite database (if using connectorx)
import polars as pl

# Simple query
df = pl.read_database_uri(
    query="SELECT * FROM users WHERE active = 1",
    uri="sqlite:///path/to/your/database.db"
)

# With file path (Windows)
df = pl.read_database_uri(
    query="SELECT * FROM sales WHERE date >= '2024-01-01'",
    uri="sqlite:///C:/data/sales.db"
)

# Alternative: Using sqlite3 + Polars (more common)
import sqlite3

conn = sqlite3.connect("database.db")
df = pl.read_database("SELECT * FROM users WHERE active = 1", connection=conn)
conn.close()

# Read entire table
df = pl.read_database_uri(
    query="SELECT * FROM products",
    uri="sqlite:///inventory.db"
)
```

## Testing Data Quality

### Comprehensive Data Validation

```python
def validate_dataframe(df: pl.DataFrame) -> dict:
    """Comprehensive data quality checks."""
    return {
        "shape": df.shape,
        "null_counts": df.null_count().to_dict(),
        "duplicates": df.is_duplicated().sum(),
        "numeric_ranges": {
            col: {
                "min": df[col].min(),
                "max": df[col].max(),
                "mean": df[col].mean()
            }
            for col in df.columns
            if df[col].dtype in [pl.Int64, pl.Float64]
        },
        "unique_categories": {
            col: df[col].n_unique()
            for col in df.columns
            if df[col].dtype == pl.Utf8
        }
    }
```

## Best Practices Summary

1. **Always prefer lazy evaluation** for files > 1GB
2. **Use expressions** (`pl.col()`) over methods
3. **Chain operations** instead of creating intermediate dataframes
4. **Leverage streaming** for memory-constrained environments
5. **Profile queries** with `.explain()` when optimizing
6. **Use proper types** - specify dtypes on read when possible
7. **Batch operations** - combine multiple `with_columns()` calls
8. **Avoid Python loops** - use vectorized operations
9. **Use Parquet** for storage and intermediate files
10. **Test with small samples** before processing full datasets
