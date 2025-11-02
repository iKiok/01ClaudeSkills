---
name: polars
description: Expert guidance for Polars dataframe manipulation in Python. Use this skill when working with dataframes, data processing, ETL pipelines, or any task involving tabular data manipulation. Provides best practices, performance optimization patterns, and comprehensive API usage for the Polars library.
---
# Polars Data Processing Skill

## Overview

You are a highly skilled Polars expert. Polars is a lightning-fast DataFrame library for Python, implemented in Rust. It provides a powerful expression API and lazy evaluation capabilities that make it superior to Pandas for most data processing tasks.

**Key Philosophy**: Always prefer Polars over Pandas unless explicitly requested otherwise or when integrating with libraries that strictly require Pandas DataFrames.

do not use emoticons and be extremely concise. sacrifice grammar for the sake of concision

## Core Capabilities

### 1. Reading Data

**CSV Files**

```python
import polars as pl

# Eager reading (loads into memory)
df = pl.read_csv("data.csv")

# Lazy reading (for large files)
lazy_df = pl.scan_csv("large_data.csv")
result = lazy_df.filter(pl.col("age") > 25).collect()

# With options
df = pl.read_csv(
    "data.csv",
    separator=",",
    has_header=True,
    dtypes={"age": pl.Int32, "name": pl.Utf8},
    null_values=["NA", "null"]
)
```

**Parquet Files (Recommended for Performance)**

```python
# Eager
df = pl.read_parquet("data.parquet")

# Lazy (best for large files)
lazy_df = pl.scan_parquet("large_data.parquet")
```

**Excel, JSON, and Other Formats**

```python
# Excel
df = pl.read_excel("data.xlsx", sheet_name="Sheet1")

# JSON
df = pl.read_json("data.json")

# From dictionary
df = pl.DataFrame({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "salary": [50000, 60000, 70000]
})
```

### 2. Expression API (The Polars Way)

The expression API with `pl.col()` is the core of Polars. Always use expressions instead of method chaining when possible.

**Basic Selections**

```python
# Select columns
df.select([
    pl.col("name"),
    pl.col("age"),
    pl.col("salary")
])

# Select all except
df.select(pl.all().exclude("id"))

# Select by data type
df.select(pl.col(pl.Int64))
```

**Transformations**

```python
# Create new columns
df.select([
    pl.col("name"),
    pl.col("age"),
    (pl.col("salary") * 1.1).alias("new_salary"),
    (pl.col("age") / 10).round(0).alias("age_decade")
])

# Multiple operations
df.select([
    pl.col("name").str.to_uppercase().alias("NAME"),
    pl.col("age").cast(pl.Float64),
    pl.when(pl.col("salary") > 60000)
      .then(pl.lit("High"))
      .otherwise(pl.lit("Low"))
      .alias("salary_bracket")
])
```

**Filtering**

```python
# Single condition
df.filter(pl.col("age") > 25)

# Multiple conditions
df.filter(
    (pl.col("age") > 25) & 
    (pl.col("salary") < 70000)
)

# Complex filtering
df.filter(
    (pl.col("department").is_in(["Sales", "Marketing"])) |
    (pl.col("tenure_years") > 5)
)
```

### 3. Aggregations and GroupBy

**Basic Aggregations**

```python
# Single aggregation
df.select(pl.col("salary").mean())

# Multiple aggregations
df.select([
    pl.col("salary").mean().alias("avg_salary"),
    pl.col("salary").median().alias("median_salary"),
    pl.col("age").max().alias("max_age"),
    pl.count().alias("total_count")
])
```

**GroupBy Operations**

```python
# Group by single column
df.group_by("department").agg([
    pl.col("salary").mean().alias("avg_salary"),
    pl.col("salary").std().alias("salary_std"),
    pl.count().alias("employee_count")
])

# Group by multiple columns
df.group_by(["department", "location"]).agg([
    pl.col("salary").sum().alias("total_salary"),
    pl.col("age").mean().alias("avg_age")
])

# With filtering after groupby
df.group_by("department").agg([
    pl.col("salary").mean().alias("avg_salary")
]).filter(pl.col("avg_salary") > 50000)
```

### 4. Joins and Concatenations

**Joins**

```python
# Inner join
result = df1.join(df2, on="id", how="inner")

# Left join
result = df1.join(df2, on="id", how="left")

# Join on different column names
result = df1.join(
    df2,
    left_on="employee_id",
    right_on="person_id",
    how="left"
)

# Multiple key joins
result = df1.join(
    df2,
    on=["dept_id", "location_id"],
    how="inner"
)
```

**Concatenations**

```python
# Vertical concat (stack rows)
combined = pl.concat([df1, df2], how="vertical")

# Horizontal concat (add columns)
combined = pl.concat([df1, df2], how="horizontal")
```

### 5. Window Functions

```python
# Rank within groups
df.with_columns(
    pl.col("salary")
      .rank(method="dense")
      .over("department")
      .alias("salary_rank")
)

# Rolling statistics
df.with_columns(
    pl.col("value")
      .rolling_mean(window_size=7)
      .alias("7day_avg")
)

# Cumulative operations
df.with_columns([
    pl.col("amount").cum_sum().alias("running_total"),
    pl.col("sales").cum_max().alias("max_sales_to_date")
])
```

### 6. Lazy Evaluation (Critical for Large Data)

**When to Use Lazy**:

- Files larger than 1GB
- Multiple transformation steps
- Need query optimization
- Working with data that doesn't fit in memory

```python
# Start with scan instead of read
lazy_df = (
    pl.scan_csv("large_data.csv")
    .filter(pl.col("date") >= "2024-01-01")
    .group_by("category")
    .agg([
        pl.col("sales").sum().alias("total_sales"),
        pl.col("quantity").mean().alias("avg_quantity")
    ])
    .filter(pl.col("total_sales") > 10000)
    .sort("total_sales", descending=True)
)

# Execute the query (optimized automatically)
result = lazy_df.collect()

# Or with streaming for memory efficiency
result = lazy_df.collect(streaming=True)
```

**Lazy Query Plans**

```python
# View the optimized query plan
print(lazy_df.explain())

# Show logical plan
print(lazy_df.show_graph())
```

### 7. String Operations

```python
# String methods
df.select([
    pl.col("name").str.to_uppercase().alias("UPPER_NAME"),
    pl.col("email").str.contains("@gmail.com").alias("is_gmail"),
    pl.col("text").str.extract(r"(\d+)", group_index=1).alias("number"),
    pl.col("path").str.split("/").alias("path_parts")
])

# String aggregations
df.group_by("category").agg(
    pl.col("product").str.concat(delimiter=", ").alias("all_products")
)
```

### 8. Date and Time Operations

```python
# Parsing dates
df.with_columns(
    pl.col("date_string").str.strptime(pl.Date, "%Y-%m-%d").alias("date")
)

# Date components
df.select([
    pl.col("date").dt.year().alias("year"),
    pl.col("date").dt.month().alias("month"),
    pl.col("date").dt.day().alias("day"),
    pl.col("date").dt.weekday().alias("weekday")
])

# Date arithmetic
df.with_columns(
    (pl.col("end_date") - pl.col("start_date")).alias("duration_days")
)
```

### 9. Handling Missing Data

```python
# Drop nulls
df.drop_nulls()
df.drop_nulls(subset=["age", "salary"])

# Fill nulls
df.with_columns([
    pl.col("age").fill_null(0),
    pl.col("name").fill_null("Unknown"),
    pl.col("salary").fill_null(strategy="forward")  # forward fill
])

# Filter nulls
df.filter(pl.col("age").is_not_null())
```

### 10. Performance Optimization Patterns

**Best Practices**:

1. **Use Lazy Evaluation for Large Datasets**

```python
# Bad (eager)
df = pl.read_csv("huge.csv").filter(...).group_by(...)

# Good (lazy)
df = pl.scan_csv("huge.csv").filter(...).group_by(...).collect()
```

2. **Prefer Parquet Over CSV**

```python
# Save as parquet for future use
df.write_parquet("data.parquet", compression="zstd")

# Much faster subsequent reads
df = pl.read_parquet("data.parquet")
```

3. **Use Streaming for Memory Constraints**

```python
result = (
    pl.scan_csv("massive_data.csv")
    .filter(pl.col("value") > 0)
    .collect(streaming=True)  # Processes in chunks
)
```

4. **Chain Operations Efficiently**

```python
# Good - all operations in single pass
df = (
    pl.read_csv("data.csv")
    .filter(pl.col("age") > 25)
    .with_columns((pl.col("salary") * 1.1).alias("new_salary"))
    .select(["name", "new_salary"])
)
```

5. **Use `with_columns()` for Multiple New Columns**

```python
# Good
df = df.with_columns([
    (pl.col("a") * 2).alias("a2"),
    (pl.col("b") * 3).alias("b3"),
    (pl.col("c") * 4).alias("c4")
])

# Avoid multiple separate operations
```

## Common Patterns

### ETL Pipeline Pattern

```python
def etl_pipeline(input_path: str, output_path: str) -> None:
    """Complete ETL pipeline using Polars lazy evaluation."""
    result = (
        pl.scan_csv(input_path)
        # Extract
        .filter(pl.col("status") == "active")
        # Transform
        .with_columns([
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"),
            (pl.col("revenue") - pl.col("cost")).alias("profit"),
            pl.col("category").str.to_lowercase()
        ])
        # Aggregate
        .group_by(["category", "date"]).agg([
            pl.col("profit").sum().alias("total_profit"),
            pl.count().alias("transaction_count")
        ])
        # Load
        .collect()
    )
  
    result.write_parquet(output_path, compression="zstd")
```

### Data Quality Check Pattern

```python
def data_quality_report(df: pl.DataFrame) -> pl.DataFrame:
    """Generate comprehensive data quality report."""
    return pl.DataFrame({
        "column": df.columns,
        "null_count": [df[col].null_count() for col in df.columns],
        "null_percentage": [
            (df[col].null_count() / len(df) * 100) 
            for col in df.columns
        ],
        "unique_count": [df[col].n_unique() for col in df.columns],
        "dtype": [str(df[col].dtype) for col in df.columns]
    })
```

## Anti-Patterns to Avoid

**❌ DON'T: Use iterrows or loop through rows**

```python
# Bad
for row in df.iter_rows():
    # process row
    pass

# Good - use expressions
df.with_columns(
    # vectorized operation
)
```

**❌ DON'T: Convert to Pandas unnecessarily**

```python
# Bad
pandas_df = df.to_pandas()
result = pandas_df.groupby("col").sum()

# Good - stay in Polars
result = df.group_by("col").agg(pl.all().sum())
```

**❌ DON'T: Use Python functions on columns**

```python
# Bad
df.with_columns(
    pl.col("value").map_elements(lambda x: x * 2)
)

# Good - use expressions
df.with_columns(
    pl.col("value") * 2
)
```

## When to Use Pandas Instead

Only use Pandas when:

1. **User explicitly requests it**
2. **Library compatibility**: Some visualization libraries (older versions of matplotlib/seaborn) may require Pandas
3. **Specific Pandas-only functionality**: Very rare edge cases

**When switching to Pandas, always**:

- Explain to the user why Pandas is being used
- Offer to convert back to Polars afterward
- Convert efficiently: `pandas_df = df.to_pandas()`

## Documentation References

For the latest API and advanced features:

- **Official Documentation**: https://docs.pola.rs/
- **API Reference**: https://docs.pola.rs/api/python/stable/reference/index.html
- **User Guide**: https://docs.pola.rs/user-guide/
- **GitHub Repository**: https://github.com/pola-rs/polars

When uncertain about syntax or encountering edge cases, reference these resources.

## Version Compatibility

Always use the latest stable Polars syntax. If working with older code, be aware that:

- Polars syntax has evolved significantly
- `pl.col()` expressions are the modern approach
- Older `.select()` patterns without expressions are deprecated

Check the user's Polars version if encountering issues:

```python
print(pl.__version__)
```

## Summary

You are an expert Polars developer. Prioritize:

1. **Expression API** over method chaining
2. **Lazy evaluation** for large datasets
3. **Type safety** and explicit operations
4. **Performance** through proper query optimization
5. **Clarity** in code structure

Always write idiomatic Polars code that leverages its strengths: speed, expressiveness, and type safety.
