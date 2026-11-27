"""Evaluation charts with Matplotlib and Seaborn (saved to figures/)."""
import json

import matplotlib
import matplotlib.ticker
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402

from bdi.config import FIGURES, RESULTS, STAGE  # noqa: E402

sns.set_theme(style="whitegrid", context="notebook")
PALETTE = {"mongodb": "#2e7d32", "sql": "#1f5fa8"}


def fig_sources(prod):
    c = prod.groupby(["source", "origin"]).size().reset_index(name="records").sort_values("records")
    c["source"] = c["source"].str.replace("www.", "", regex=False)
    fig, ax = plt.subplots(figsize=(9, 7))
    ax.barh(c["source"], c["records"], color=c["origin"].map(PALETTE))
    ax.set_xscale("log")
    ax.set_xlabel("Product listings (log scale)")
    ax.set_title("Listings per website by source system")
    handles = [plt.Rectangle((0, 0), 1, 1, color=v) for v in PALETTE.values()]
    ax.legend(handles, ["MongoDB (JSON documents)", "SQL (relational tables)"], loc="lower right")
    fig.tight_layout()
    fig.savefig(FIGURES / "fig1_sources.png", dpi=200)
    plt.close(fig)


def fig_heterogeneity(rows, prod):
    top = rows["attribute"].value_counts().head(15).index
    n = prod.groupby("source").size()
    share = (rows[rows.attribute.isin(top)].groupby(["source", "attribute"])["spec_id"].nunique()
             .div(n, level="source").unstack(fill_value=0).reindex(columns=top))
    share.index = share.index.str.replace("www.", "", regex=False)
    fig, ax = plt.subplots(figsize=(11, 8))
    sns.heatmap(share, cmap="Blues", vmin=0, vmax=1, cbar_kws={"label": "Share of listings with attribute"}, ax=ax)
    ax.set_xlabel("Attribute name (15 most frequent)")
    ax.set_ylabel("Website")
    ax.set_title("Schema heterogeneity: the same attribute is present, absent, or renamed across websites")
    fig.tight_layout()
    fig.savefig(FIGURES / "fig2_heterogeneity.png", dpi=200)
    plt.close(fig)


def fig_quality(prod):
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    attr_only = {"Brand": (prod.brand.isna() | prod.brand_from_title).mean(),
                 "Screen size": (prod.screen_in.isna() | prod.size_from_title).mean()}
    after = {"Brand": prod.brand.isna().mean(), "Screen size": prod.screen_in.isna().mean()}
    d = pd.DataFrame({"Attributes only": attr_only, "After title parsing": after}).mul(100)
    d.plot.bar(ax=axes[0], rot=0, color=["#bdbdbd", "#1f5fa8"])
    axes[0].set_ylabel("Listings missing the value (%)")
    axes[0].set_title("Missing values before and after recovery from titles")
    for c in axes[0].containers:
        axes[0].bar_label(c, fmt="%.1f%%")
    sns.histplot(prod.screen_in.dropna(), bins=40, ax=axes[1], color="#1f5fa8")
    axes[1].set_xlabel("Screen size (inches, normalized from in, cm and title text)")
    axes[1].set_title("Distribution of normalized screen sizes")
    fig.tight_layout()
    fig.savefig(FIGURES / "fig3_quality.png", dpi=200)
    plt.close(fig)


def fig_integration(summary):
    sm, er = summary["schema_matching"], summary["entity_resolution"]["pairwise"]
    d = pd.DataFrame({
        "Schema matching: name similarity": sm["name_similarity"],
        "Schema matching: learned dictionary": sm["learned_dictionary"],
        "Entity resolution: model-code blocking": er}).loc[["precision", "recall", "f1"]].T
    ax = d.plot.bar(figsize=(10, 4.8), rot=0, color=["#90caf9", "#1f5fa8", "#2e7d32"])
    ax.set_ylim(0, 1.05)
    ax.set_ylabel("Score against Alaska ground truth")
    ax.set_title("Integration quality")
    ax.set_xticklabels([t.get_text().replace(": ", ":\n") for t in ax.get_xticklabels()])
    for c in ax.containers:
        ax.bar_label(c, fmt="%.2f", fontsize=9)
    ax.legend(["Precision", "Recall", "F1"], loc="upper left")
    plt.tight_layout()
    plt.savefig(FIGURES / "fig4_integration.png", dpi=200)
    plt.close()


def fig_benchmark():
    b = pd.read_csv(RESULTS / "benchmark.csv")
    fig, ax = plt.subplots(figsize=(9.5, 4.8))
    sns.lineplot(data=b, x="rows", y="seconds", hue="engine", marker="o", ax=ax)
    ax.set_xlabel("Rows processed (1x, 4x, 10x the Alaska monitor data)")
    ax.set_yscale("log")
    ax.set_ylabel("Execution time (seconds, log scale)")
    ax.set_title("Same aggregation in four engines")
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1e6:.1f} M"))
    for e, g in b.groupby("engine"):
        last = g.iloc[-1]
        ax.annotate(f"{last.seconds:.2f} s", (last.rows, last.seconds), textcoords="offset points",
                    xytext=(6, 0), va="center", fontsize=9)
    ax.set_xlim(right=b.rows.max() * 1.15)
    ax.legend(title="Engine", loc="upper left", bbox_to_anchor=(1.01, 1))
    fig.tight_layout()
    fig.savefig(FIGURES / "fig5_benchmark.png", dpi=200)
    plt.close(fig)


def fig_stream():
    p = RESULTS / "stream_by_brand.csv"
    if not p.exists():
        return
    s = pd.read_csv(p).head(12)
    fig, ax = plt.subplots(figsize=(8, 4.5))
    sns.barplot(data=s, y="brand", x="count", color="#2e7d32", ax=ax)
    ax.set_title("Listings per brand computed by Spark Structured Streaming from Kafka")
    ax.set_xlabel("Messages")
    ax.set_ylabel("Brand detected in the listing title ('other' = none of 12 brands)")
    fig.tight_layout()
    fig.savefig(FIGURES / "fig6_streaming.png", dpi=200)
    plt.close(fig)


def run():
    prod = pd.read_parquet(STAGE / "products.parquet")
    rows = pd.read_parquet(STAGE / "attributes_clean.parquet")
    summary = json.loads((RESULTS / "summary.json").read_text())
    fig_sources(prod)
    fig_heterogeneity(rows, prod)
    fig_quality(prod)
    fig_integration(summary)
    fig_benchmark()
    fig_stream()
