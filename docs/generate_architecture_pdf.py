"""Generate architecture summary (Markdown) and PDF.

Outputs:
  - docs/askmydocs-architecture-summary.md  (human-readable summary)
  - docs/askmydocs-architecture.pdf           (rendered from the same content)

Usage:
  python docs/generate_architecture_pdf.py
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from fpdf import FPDF

DOCS = Path(__file__).resolve().parent
SUMMARY_MD = DOCS / "askmydocs-architecture-summary.md"
SUMMARY_PDF = DOCS / "askmydocs-architecture.pdf"

BlockKind = Literal[
    "title",
    "subtitle",
    "exec_summary",
    "h1",
    "h2",
    "body",
    "bullet",
    "code",
    "table_header",
    "table_row",
]


@dataclass(frozen=True)
class Block:
    kind: BlockKind
    text: str = ""
    col1: str = ""
    col2: str = ""


def build_content() -> list[Block]:
    """Single source of truth for summary Markdown and PDF."""
    return [
        Block("title", "askmydocs"),
        Block(
            "subtitle",
            "Project Architecture, Data Flow & Design Decisions",
        ),
        Block(
            "exec_summary",
            "askmydocs is a batch data-catalog pipeline that ingests ~500 YAML data "
            "dictionary files (real masked samples plus synthetic neighbors), validates "
            "them locally and in Spark, and writes clean Parquet plus a quarantine lane "
            "and DQ report. The design prioritizes defensive parsing, explicit schemas, "
            "and two-tier data quality (row quarantine vs batch halt). It is the foundation "
            'for a future "ask my docs" RAG layer; the chat UI is not built yet.',
        ),
        Block("h1", "1. High-Level Architecture"),
        Block(
            "body",
            "The system is layered: contract -> raw YAML -> validation -> Spark ingest -> Parquet outputs.",
        ),
        Block(
            "code",
            """
[Contract]  JSON Schema, domain_topology.md, dq_contract.md
     |
[Sources]   data/real/*.yaml  +  generate_yamls.py  ->  data/raw/ (~500 files)
     |
[Local QA]  validate_real.py, pytest (no Spark)
     |
[Spark]     Docker -> spark_session -> ingest_catalog.py
     |
[Outputs]   catalog/  |  quarantine/  |  dq_report/report.json
""",
        ),
        Block("h2", "Layers"),
        Block("table_header", col1="Layer", col2="Role"),
        Block("table_row", col1="Schemas", col2="JSON Schema + domain topology define valid dictionary entries"),
        Block("table_row", col1="Data", col2="~500 YAML files mimicking a logistics warehouse data dictionary"),
        Block("table_row", col1="Local tools", col2="Generate data, validate real files, pytest on full corpus"),
        Block("table_row", col1="Spark jobs", col2="Batch parse, row-level DQ, aggregate contract, write Parquet"),
        Block("table_row", col1="Docker", col2="Reproducible local Spark (Java 17 + PySpark 3.5.3)"),
        Block("body", "main.py is a stub; real entry points are scripts, tests, and spark_jobs/."),
        Block("h1", "2. What Each YAML Represents"),
        Block("body", "Each file documents one warehouse table:"),
        Block("bullet", "table_name - e.g. pets_tickets_enriched"),
        Block("bullet", "overview - purpose, granularity, business_rules"),
        Block("bullet", "input_tables - lineage (namespace.table, e.g. data_warehouse.shipments)"),
        Block("bullet", "fields - name, description, source path(s), technical_description (SQL-ish)"),
        Block("h2", "Domain model (schemas/domain_topology.md)"),
        Block(
            "bullet",
            "10 business domains: orders, shipments, hubs, fleet, customers, support, "
            "billing, inventory, calendar, audit",
        ),
        Block("bullet", "Table archetypes: *_enriched, *_events, dim_*/fct_*, stg_*, etc."),
        Block("bullet", "Namespaces: data_warehouse, core_prod_gl, metadata"),
        Block("h1", "3. End-to-End Data Flow"),
        Block("h2", "Step 1: Build raw corpus (~500 files)"),
        Block(
            "code",
            """
data/real/*.yaml  --copy-->\\
                           +--> data/raw/*.yaml (target: 500)
generate_yamls.py --synthetic-->/
""",
        ),
        Block(
            "body",
            "generate_yamls.py copies real masked YAMLs, generates synthetic tables to reach 500, "
            "uses Faker + domain vocabulary, validates each synthetic doc with jsonschema before write.",
        ),
        Block("h2", "Step 2: Local validation (pre-Spark)"),
        Block("bullet", "validate_real.py - schema-check real files only"),
        Block(
            "bullet",
            "tests/test_yaml_validity.py - count=500, schema, duplicates, source paths, lineage coverage",
        ),
        Block("h2", "Step 3: Production ingest (spark_jobs/ingest_catalog.py)"),
        Block("bullet", "1. Read - binaryFile + *.yaml glob (one row per file)"),
        Block("bullet", "2. Parse - Python UDF -> PARSED_SCHEMA (parse errors captured, not thrown)"),
        Block("bullet", "3. Validate - Spark SQL CASE WHEN -> rejection_reason"),
        Block("bullet", "4. Report - dq_report/report.json"),
        Block("bullet", "5. Halt - DQContractViolation if aggregate thresholds fail"),
        Block("bullet", "6. Write - good -> catalog/, bad -> quarantine/"),
        Block("body", "Sample run: 500 files, 484 good, 16 quarantined (3.2% rejection rate)."),
        Block("h2", "Step 4: Run environment"),
        Block("bullet", "run.bat -> docker compose run spark"),
        Block("bullet", "Container mounts repo at /app, PYTHONPATH=/app"),
        Block("bullet", "get_spark(): local[4], 4g driver, UTC, AQE, UI on port 4040"),
        Block("bullet", "Production path: GKE + Spark Operator (planned)"),
        Block("h1", "4. Ingestion Design: Three Approaches"),
        Block("table_header", col1="Script", col2="Approach / status"),
        Block("table_row", col1="ingest_v1_text_udf.py", col2="binaryFile + UDF - reliable one-row-per-file"),
        Block(
            "table_row",
            col1="ingest_v2_wholetextfiles.py",
            col2="RDD wholeTextFiles - schema-inference pitfalls demo",
        ),
        Block("table_row", col1="ingest_catalog.py", col2="CANONICAL - v1 parsing + full DQ + Parquet"),
        Block(
            "body",
            "Chosen pattern: binaryFile + explicit StructType + defensive parsing "
            "(coerce input_tables string to list, filter business_rules to strings). "
            "Row-level issues go to quarantine; job only fails on aggregate breach.",
        ),
        Block("h1", "5. Data Quality: Two-Tier Model"),
        Block("body", "Documented in spark_jobs/dq_contract.md, implemented in ingest_catalog.py."),
        Block("h2", "Per-row (quarantine, job continues)"),
        Block(
            "body",
            "Parse failure, bad table_name, empty fields, duplicate field names, "
            "short purpose/granularity, malformed input_tables.",
        ),
        Block("h2", "Aggregate (job fails, no Parquet written)"),
        Block("table_header", col1="Rule", col2="Threshold"),
        Block("table_row", col1="Volume", col2="file_count < 400"),
        Block("table_row", col1="Rejection rate", col2="> 5%"),
        Block(
            "table_row",
            col1="Single-cause dominance",
            col2="> 50% of rejections from one reason (if >= 5 bad)",
        ),
        Block("table_row", col1="Namespace diversity", col2="< 2 distinct input namespaces in good rows"),
        Block(
            "body",
            "Note: dq_contract.md mentions 8 business domains; implemented check is 2+ input "
            "namespaces (data_warehouse vs core_prod_gl). distinct_input_namespaces in reports "
            "counts namespace prefixes, not the 10 domains in domain_topology.md.",
        ),
        Block("h2", "Explicitly out of scope for ingest"),
        Block("bullet", "Whether referenced tables exist (cross-file lineage - Day 4)"),
        Block("bullet", "Semantic correctness of descriptions (needs LLM)"),
        Block("bullet", "Schema evolution over time (planned: dbt snapshots)"),
        Block("body", "scripts/corrupt_for_dq_testing.py breaks 9 files to test DQ behavior."),
        Block("h1", "6. Schema and Testing Decisions"),
        Block("bullet", "JSON Schema is source of truth for generation and pytest"),
        Block(
            "bullet",
            "Spark DQ adds stricter/different rules - intentional split: fast local vs distributed batch",
        ),
        Block(
            "bullet",
            "Synthetic data: weighted domains, cross-domain input_tables, derived fields with SQL-like tech descriptions",
        ),
        Block(
            "bullet",
            "Same pipeline for real + synthetic so downstream dbt/RAG does not special-case source",
        ),
        Block("h1", "7. Dependencies and Operations"),
        Block("table_header", col1="Tool", col2="Use"),
        Block("table_row", col1="uv / pyproject.toml", col2="faker, jsonschema, pyyaml, pytest (Python >= 3.12)"),
        Block("table_row", col1="Docker Spark image", col2="pyspark 3.5.3, pyyaml, jsonschema in container"),
        Block("table_row", col1="Parquet", col2="Analytics-ready catalog for later search/embedding"),
        Block("h1", "8. Roadmap (Implied, Not Built Yet)"),
        Block("bullet", "Week 2: dbt snapshots for schema evolution"),
        Block("bullet", "Day 4: cross-file lineage integrity"),
        Block("bullet", "Week 3 Day 5: GKE Spark Operator"),
        Block(
            "bullet",
            "RAG / ask my docs: retrieve from catalog Parquet using purpose, business_rules, field text",
        ),
        Block("h1", "9. Summary"),
        Block(
            "body",
            "askmydocs is a batch data-catalog pipeline: YAML data dictionaries -> validated, "
            "queryable Parquet with quarantine and halt-on-bad-batch semantics. Design emphasizes "
            "defensive parsing, explicit schemas, two-tier DQ, Docker-local Spark with a path to "
            "Kubernetes, and a 500-file corpus blending real masked docs with synthetic logistics "
            "tables for downstream analytics and Q&A.",
        ),
    ]


def write_summary_markdown(blocks: list[Block], path: Path) -> None:
    """Write the Markdown summary file."""
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines: list[str] = [
        "<!-- Auto-generated by docs/generate_architecture_pdf.py; do not edit by hand. -->",
        f"<!-- Generated: {generated} -->",
        "",
    ]

    for i, block in enumerate(blocks):
        if block.kind == "title":
            lines.append(f"# {block.text}")
        elif block.kind == "subtitle":
            lines.append(f"## {block.text}")
            lines.append("")
            lines.append(f"*Generated: {generated}*")
        elif block.kind == "exec_summary":
            lines.append("")
            lines.append("## Executive Summary")
            lines.append("")
            lines.append(block.text)
        elif block.kind == "h1":
            lines.extend(["", f"## {block.text}", ""])
        elif block.kind == "h2":
            lines.extend(["", f"### {block.text}", ""])
        elif block.kind == "body":
            if i > 0 and blocks[i - 1].kind == "table_row":
                lines.append("")
            lines.extend([block.text, ""])
        elif block.kind == "bullet":
            lines.append(f"- {block.text}")
        elif block.kind == "code":
            lines.extend(["```text", block.text.strip(), "```", ""])
        elif block.kind == "table_header":
            lines.extend(
                [
                    f"| {block.col1} | {block.col2} |",
                    "| --- | --- |",
                ]
            )
        elif block.kind == "table_row":
            lines.append(f"| {block.col1} | {block.col2} |")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


class ArchPDF(FPDF):
    def header(self) -> None:
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(100, 100, 100)
            self.cell(0, 8, "askmydocs - Architecture Overview", align="R")
            self.ln(12)

    def footer(self) -> None:
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def h1(self, text: str) -> None:
        self.ln(4)
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(30, 60, 120)
        self.multi_cell(0, 8, text)
        self.ln(2)

    def h2(self, text: str) -> None:
        self.ln(2)
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 7, text)
        self.ln(1)

    def body(self, text: str) -> None:
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5, text)
        self.ln(2)

    def bullet(self, text: str) -> None:
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        x = self.get_x()
        self.cell(6, 5, "-")
        self.multi_cell(0, 5, text)
        self.set_x(x)

    def code_block(self, text: str) -> None:
        self.set_fill_color(245, 245, 245)
        self.set_font("Courier", "", 8)
        self.set_text_color(20, 20, 20)
        for line in text.strip().splitlines():
            self.cell(0, 4.5, "  " + line, new_x="LMARGIN", new_y="NEXT", fill=True)
        self.ln(3)
        self.set_font("Helvetica", "", 10)

    def table_row(self, col1: str, col2: str, bold: bool = False) -> None:
        style = "B" if bold else ""
        self.set_font("Helvetica", style, 9)
        w1, w2 = 55, 135
        y0 = self.get_y()
        x0 = self.get_x()
        self.multi_cell(w1, 5, col1, border=1)
        h1 = self.get_y() - y0
        self.set_xy(x0 + w1, y0)
        self.multi_cell(w2, 5, col2, border=1)
        h2 = self.get_y() - y0
        self.set_xy(x0, y0 + max(h1, h2))

    def title_page(self, title: str, subtitle: str, exec_summary: str) -> None:
        self.add_page()
        self.set_font("Helvetica", "B", 22)
        self.set_text_color(30, 60, 120)
        self.cell(0, 16, title, new_x="LMARGIN", new_y="NEXT")
        self.set_font("Helvetica", "", 14)
        self.set_text_color(60, 60, 60)
        self.cell(0, 10, subtitle, new_x="LMARGIN", new_y="NEXT")
        self.ln(6)
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(40, 40, 40)
        self.cell(0, 8, "Executive Summary", new_x="LMARGIN", new_y="NEXT")
        self.ln(2)
        self.set_font("Helvetica", "", 11)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 6, exec_summary)


def write_pdf(blocks: list[Block], path: Path) -> Path:
    """Render PDF from the same content blocks as the Markdown summary. Returns path written."""
    pdf = ArchPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)

    title = subtitle = exec_summary = ""
    for block in blocks:
        if block.kind == "title":
            title = block.text
        elif block.kind == "subtitle":
            subtitle = block.text
        elif block.kind == "exec_summary":
            exec_summary = block.text
            break

    pdf.title_page(title, subtitle, exec_summary)
    pdf.add_page()
    started_body = False

    for block in blocks:
        if block.kind in ("title", "subtitle", "exec_summary"):
            continue
        if block.kind == "h1" and not started_body:
            started_body = True
        elif block.kind == "h1" and pdf.get_y() > 240:
            pdf.add_page()

        if block.kind == "h1":
            pdf.h1(block.text)
        elif block.kind == "h2":
            pdf.h2(block.text)
        elif block.kind == "body":
            pdf.body(block.text)
        elif block.kind == "bullet":
            pdf.bullet(block.text)
        elif block.kind == "code":
            pdf.code_block(block.text)
        elif block.kind == "table_header":
            pdf.table_row(block.col1, block.col2, bold=True)
        elif block.kind == "table_row":
            pdf.table_row(block.col1, block.col2)

    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        pdf.output(str(path))
        return path
    except PermissionError:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        fallback = path.with_stem(f"{path.stem}-{stamp}")
        pdf.output(str(fallback))
        print(
            f"WARNING: Could not overwrite {path} (file may be open). "
            f"Wrote PDF to {fallback} instead."
        )
        return fallback


def main() -> None:
    blocks = build_content()

    write_summary_markdown(blocks, SUMMARY_MD)
    print(f"Wrote summary: {SUMMARY_MD} ({SUMMARY_MD.stat().st_size:,} bytes)")

    pdf_path = write_pdf(blocks, SUMMARY_PDF)
    print(f"Wrote PDF:     {pdf_path} ({pdf_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
