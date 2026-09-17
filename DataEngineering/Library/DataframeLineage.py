import contextlib
import re
import io

from pyspark.sql import DataFrame

import sempy.fabric as fabric

def get_execution_plan(df: DataFrame, extended: bool = None, mode: str = None) -> str:
    """Return a Spark DataFrame's execution plan as a string.

    Temporarily redirects standard output to capture the plan printed by
    ``DataFrame.explain()``. Spark's plan-string and field limits are increased
    to reduce truncation of large execution plans.

    Args:
        df: Spark DataFrame whose execution plan should be retrieved.
        extended: Whether to include parsed, analyzed, optimized, and physical
            plans. Passed directly to ``DataFrame.explain()``.
        mode: Explain output mode, such as ``"simple"``, ``"extended"``,
            ``"codegen"``, ``"cost"``, or ``"formatted"``. Passed directly to
            ``DataFrame.explain()``.

    Returns:
        The execution plan printed by Spark.
    """
    with contextlib.redirect_stdout(io.StringIO()) as stdout:
        df.explain(extended = extended, mode = mode)

        plan = stdout.getvalue()
        
    return plan


def extract_table_dependencies(plan_text: str) -> list[dict[str, str]]:
    """Extract table dependencies from a Spark execution plan.

    Searches the plan for ``FileScan`` operations and extracts identifiers
    beginning with ``<database>.<namespace>``. Table names may contain spaces,
    periods, and other characters appearing before the scan's column list.

    Duplicate identifiers are removed and returned in alphabetical order.

    Args:
        plan_text: Spark execution plan text, typically produced by
            ``DataFrame.explain()``.

    Returns:
        Dependency dictionaries containing the keys ``"database"``,
        ``"namespace"``, and ``"table"``.

    Raises:
        ValueError: If a matched identifier does not contain at least three
            dot-separated components.
    """
    # Example:
    # FileScan parquet spark_catalog.<namespace>.date v2.new[col1,col2,...]
    filescan_pattern = re.compile(
        r"FileScan\s+\S+\s+(?P<table>[^\[\r\n]+?)\["
    )

    identifiers = {
        match.group("table").strip()
        for match in filescan_pattern.finditer(plan_text)
    }

    dependencies = []

    for identifier in sorted(identifiers):
        database, namespace, table = identifier.split(".", maxsplit=2)
        dependencies.append(
            {
                "database": database,
                "namespace": namespace,
                "table": table,
            }
        )

    return dependencies


def get_source_tables(
    df: DataFrame,
    as_objects: bool = False,
    exclude_workspace: bool = False,
) -> list[str] | list[dict[str, str]]:
    """Return the source tables referenced by a Spark DataFrame.

    Extracts table dependencies from the DataFrame's execution plan and
    resolves Microsoft Fabric's internal namespace identifiers to their
    user-friendly workspace, lakehouse, and schema names using
    ``DESCRIBE DATABASE``.

    Args:
        df: Spark DataFrame whose source-table dependencies are inspected.
        as_objects: If ``True``, return each source table as a dictionary.
            If ``False``, return each table as a backtick-quoted qualified
            name.
        exclude_workspace: If ``True``, omit the workspace component from
            each returned table name or dictionary.

    Returns:
        Source tables in the deterministic order produced by
        ``extract_table_dependencies()``.

        With ``exclude_workspace=False``, qualified names have the form
        ``"`<workspace>`.`<lakehouse>`.`<schema>`.`<table>`"``.

        With ``exclude_workspace=True``, qualified names have the form
        ``"`<lakehouse>`.`<schema>`.`<table>`"``.

        When ``as_objects=True``, each item is a dictionary containing the
        corresponding components.

    Note:
        This function executes one ``DESCRIBE DATABASE`` query for each
        distinct internal namespace found in the execution plan.
    """
    dependencies = extract_table_dependencies(get_execution_plan(df))

    # resolve fabric internal namespaces to userfriendly names (<LH>.<schema>)
    namespace_mapping = {} 
    array_start = 1 if exclude_workspace else 0

    # get distinct namespaces/databases/schemas
    namespaces = set([d["namespace"] for d in dependencies])

    for namespace in namespaces:
        namespace_info = {row["info_name"]: row["info_value"] for row in df.sparkSession.sql(f"DESCRIBE DATABASE {namespace}").collect()}
        # remove the workspace name and quotes
        parts = [p.strip('`') for p in namespace_info["Namespace Name"].split("`.`")]
        if len(parts) == 1: # for non-schema-enabled lakehouses we need to derive the workspace from the onelake location
            workspace = fabric.resolve_workspace_name(namespace_info["Location"][8:44])
            namespace_mapping[namespace] = {
                "workspace": workspace,
                "lakehouse": namespace_info["Namespace Name"],
                "schema": "dbo", # always "dbo" for non-schema-enabled lakehouses
            }
        else:
            namespace_mapping[namespace] = dict(list(zip(["workspace", "lakehouse", "schema"], parts))[array_start:])

    source_tables = []
    for d in dependencies:
        source_tables.append(namespace_mapping[d["namespace"]] | {"table": d["table"]})

    if as_objects:
        return source_tables
    else:
        return [
            ".".join(f"`{v}`" for v in st.values())
            for st in source_tables
        ]


df = spark.sql("""
SELECT * 
FROM `FABRIC Playground`.paiqo_playground.`date v2.new`
CROSS JOIN `FABRIC Playground`.paiqo_playground.`flo_test` 
CROSS JOIN AdventureWorksLH.dbo.dimcurrency
LIMIT 1000
""")

get_source_tables(df, False)
