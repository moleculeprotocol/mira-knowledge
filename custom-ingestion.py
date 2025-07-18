import json
import time

import lancedb
import pandas as pd
from dotenv import load_dotenv
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector
from langchain_text_splitters import (
    MarkdownHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)


def wait_for_index(table, index_name):
    POLL_INTERVAL = 10
    while True:
        indices = table.list_indices()

        if indices and any(index.name == index_name for index in indices):
            break
        print(f"⏳ Waiting for {index_name} to be ready...")
        time.sleep(POLL_INTERVAL)

    print(f"✅ {index_name} is ready!")


load_dotenv()

headers_to_split_on = [
    ("#", "header_1"),
    ("##", "header_2"),
    ("###", "header_3"),
    ("####", "header_4"),
]

markdown_splitter = MarkdownHeaderTextSplitter(
    headers_to_split_on=headers_to_split_on, strip_headers=False
)

files = [
    {
        "path": "data/molecule-docs.json",
        "source": "Molecule Documentation",
    },
    {
        "path": "data/molecule-blog.json",
        "source": "Molecule Blog",
    },
    {
        "path": "data/desci-codes.json",
        "source": "DeSci.Codes",
    },
]

parsed_data = []

for file in files:
    # Open and parse the JSON file
    with open(file["path"], "r", encoding="utf-8") as f:
        data = json.load(f)

    # Extract URL and title from each item
    for item in data:
        title = item.get("metadata", {}).get("title", "N/A")
        content = item.get("markdown", "")
        url = item.get("url", "N/A")
        metadata = {"url": url, "source": file["source"]}

        parsed_data.append({"title": title, "content": content, "metadata": metadata})


# Create a pandas DataFrame
df = pd.DataFrame(parsed_data)

chunk_size = 2000
chunk_overlap = 200
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=chunk_size, chunk_overlap=chunk_overlap
)

chunks = []

for i, row in df.iterrows():
    md_header_splits = markdown_splitter.split_text(row["content"])
    splits = text_splitter.split_documents(md_header_splits)
    for i, split in enumerate(splits):
        split.metadata["page_title"] = row["title"]
        split.metadata["url"] = row["metadata"]["url"]
        split.metadata["source"] = row["metadata"]["source"]
        chunks.append(split)

print(f"Number of chunks: {len(chunks)}")

# --------------------------------------------------------------
# Create a LanceDB database and table
# --------------------------------------------------------------

# Create a LanceDB database
db = lancedb.connect("db/lancedb")


# Get the OpenAI embedding function
func = get_registry().get("openai").create(name="text-embedding-3-large")


# Define a simplified metadata schema
class ChunkMetadata(LanceModel):
    """
    You must order the fields in alphabetical order.
    This is a requirement of the Pydantic implementation.
    """

    header_1: str | None
    header_2: str | None
    header_3: str | None
    header_4: str | None
    page_title: str | None
    source: str | None
    url: str | None


# Define the main Schema
class Chunks(LanceModel):
    text: str = func.SourceField()
    vector: Vector(func.ndims()) = func.VectorField()  # type: ignore
    metadata: ChunkMetadata


table = db.create_table("molrag", schema=Chunks, mode="overwrite")


# --------------------------------------------------------------
# Prepare the chunks for the table
# --------------------------------------------------------------

# Create table with processed chunks
processed_chunks = [
    {
        "text": chunk.page_content,
        "metadata": {
            "header_1": chunk.metadata["header_1"]
            if "header_1" in chunk.metadata
            else None,
            "header_2": chunk.metadata["header_2"]
            if "header_2" in chunk.metadata
            else None,
            "header_3": chunk.metadata["header_3"]
            if "header_3" in chunk.metadata
            else None,
            "header_4": chunk.metadata["header_4"]
            if "header_4" in chunk.metadata
            else None,
            "page_title": chunk.metadata["page_title"],
            "source": chunk.metadata["source"],
            "url": chunk.metadata["url"],
        },
    }
    for chunk in chunks
]

# --------------------------------------------------------------
# Add the chunks to the table (automatically embeds the text)
# --------------------------------------------------------------

table.add(processed_chunks)

table.create_fts_index("text", use_tantivy=False, with_position=True, replace=True)

# Wait for indexes to be ready
# wait_for_index(table, "text_idx")

# Create index with cosine similarity
# Note: vector_column_name only needed for multiple vector columns or non-default names
# Supported index types: IVF_PQ (default) and IVF_HNSW_SQ
table.create_index(metric="cosine")

# --------------------------------------------------------------
# Load the table
# --------------------------------------------------------------

table.to_pandas()
table.count_rows()
