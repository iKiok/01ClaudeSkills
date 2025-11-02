# Polars Skill for Claude Code

This skill transforms Claude into an expert Polars developer, providing comprehensive guidance for dataframe manipulation, data processing, and ETL pipelines.

## What This Skill Does

When activated, Claude will:
- ✅ **Always prefer Polars** over Pandas for data processing tasks
- ✅ Use **idiomatic Polars patterns** with the expression API
- ✅ Apply **performance optimizations** automatically (lazy evaluation, streaming)
- ✅ Provide **type-safe** and efficient code
- ✅ Reference **official documentation** when needed
- ✅ Explain trade-offs and best practices

## Installation

### For Claude Desktop / Chat
Copy this entire folder to your skills directory and enable it in your Claude settings.

### For Claude Code

#### Option 1: User-level (All Projects)
```bash
# Copy the entire polars-skill folder to:
~/.claude/skills/polars/

# Or on Windows:
C:\Users\[YourUsername]\.claude\skills\polars\
```

#### Option 2: Project-level (Specific Project)
```bash
# Copy to your project directory:
your-project/.claude/skills/polars/
```

## Skill Structure

```
polars-skill/
├── SKILL.md                      # Main skill file (loaded by Claude)
├── references/
│   └── advanced-patterns.md      # Advanced patterns and edge cases
└── README.md                     # This file (for humans, not loaded by Claude)
```

## Usage Examples

Once installed, Claude will automatically use this skill when you ask about data processing. For example:

### Example 1: Basic Data Processing
```
User: "Read this CSV file and filter for rows where age > 25, then calculate average salary by department"

Claude: [Uses Polars automatically with proper expressions and lazy evaluation]
```

### Example 2: ETL Pipeline
```
User: "Create an ETL pipeline that processes sales data"

Claude: [Uses Polars scan/collect pattern with proper transformations]
```

### Example 3: Performance Optimization
```
User: "This data processing is slow, can you optimize it?"

Claude: [Applies lazy evaluation, streaming, and Parquet conversion]
```

## When Claude Uses This Skill

The skill is triggered when your query involves:
- Dataframes or tabular data
- CSV, Parquet, Excel, or JSON files
- Data processing, filtering, or aggregation
- ETL pipelines or data transformations
- GroupBy operations or joins
- Time series analysis

## Key Features

### 1. Expression API First
```python
# Claude will write this:
df.select([
    pl.col("name"),
    (pl.col("salary") * 1.1).alias("new_salary")
])

# Not this:
df["new_salary"] = df["salary"] * 1.1
```

### 2. Lazy Evaluation for Large Data
```python
# Claude automatically uses lazy evaluation when appropriate:
result = (
    pl.scan_csv("large_file.csv")
    .filter(pl.col("value") > 0)
    .group_by("category")
    .agg(pl.col("amount").sum())
    .collect()
)
```

### 3. Performance Optimization
```python
# Claude will suggest Parquet for better performance:
df.write_parquet("data.parquet", compression="zstd")
```

### 4. Proper Error Handling
```python
# Claude includes safe operations:
df.with_columns(
    pl.col("value").cast(pl.Float64, strict=False).fill_null(0)
)
```

## Advanced Usage

### Load Advanced Patterns
If you need advanced patterns, ask Claude to reference them:

```
"Read the advanced patterns reference and show me how to do a rolling window calculation"
```

Claude will read `references/advanced-patterns.md` and apply those patterns.

### Override Polars Preference
If you specifically need Pandas:

```
"Use Pandas for this task because I need to integrate with seaborn"
```

Claude will use Pandas and explain why.

## Customization

You can customize this skill by:

1. **Editing SKILL.md** - Add your company-specific patterns
2. **Adding references/** - Add your own reference documents
3. **Adding scripts/** - Add reusable Python scripts for common tasks

## Updating CLAUDE.md

If you have a CLAUDE.md file in your project, you can reference this skill:

```markdown
# Python Development Standards

## Data Processing
- For all dataframe operations, follow the patterns in the Polars skill
- Always use lazy evaluation for files > 1GB
```

## Documentation Links

The skill references official Polars documentation:
- [Official Docs](https://docs.pola.rs/)
- [API Reference](https://docs.pola.rs/api/python/stable/reference/index.html)
- [User Guide](https://docs.pola.rs/user-guide/)
- [GitHub](https://github.com/pola-rs/polars)

## Troubleshooting

### Skill Not Triggering
- Make sure the skill folder is in the correct location
- Check that SKILL.md has proper YAML frontmatter
- Try explicitly mentioning "dataframe" or "data processing" in your query

### Claude Still Uses Pandas
- Explicitly state "use Polars" in your request
- Check if there's a valid reason (library compatibility)
- Review your CLAUDE.md file for conflicting instructions

### Performance Issues
- Ask Claude to "optimize this for performance"
- Request lazy evaluation: "use lazy evaluation for this"
- Ask for profiling: "explain this query plan"

## Contributing

To improve this skill:
1. Test it on real projects
2. Note what works well and what doesn't
3. Update SKILL.md with new patterns
4. Add examples to references/

## Version

Version: 1.0.0
Created for: Claude Code
Compatible with: Polars 0.18.0+

## License

This skill is provided as-is for use with Claude Code. Feel free to modify and distribute.

---

**Pro Tip**: Start with simple queries and let Claude show you idiomatic Polars patterns. Then build on those patterns for more complex tasks!
